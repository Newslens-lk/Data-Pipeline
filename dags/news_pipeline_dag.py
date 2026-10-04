"""
News pipeline DAG — supports both local Docker and AWS ECS execution.

Each task launches a standalone container. Containers communicate through
object storage (MinIO locally, S3 on AWS) via NDJSON files.
The storage key each container prints to stdout becomes its XCom value,
which downstream tasks receive as environment variables.

Pipeline:  scrape -> clean -> embed -> [bias, cluster] -> load
                                        (parallel)

Deploy target (set Airflow Variable "deploy_target" to "aws" or "local"):
  - local (default): DockerOperator on the host Docker daemon
  - aws:             EcsRunTaskOperator on Fargate
"""

from __future__ import annotations

import datetime as dt
import os

from airflow.decorators import dag
from airflow.models import Variable

# ---------------------------------------------------------------------------
# Deploy target: "local" (default) or "aws"
# ---------------------------------------------------------------------------
DEPLOY_TARGET = Variable.get("deploy_target", default_var="local")

if DEPLOY_TARGET == "aws":
    from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
else:
    from airflow.providers.docker.operators.docker import DockerOperator

# ---------------------------------------------------------------------------
# Default args applied to every task unless overridden.
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": dt.timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": dt.timedelta(minutes=30),
}

# ---------------------------------------------------------------------------
# Shared environment variables — loaded from an Airflow Variable called
# "pipeline_env" (a JSON dict stored in Airflow's metadata DB, encrypted
# with the Fernet key).
# ---------------------------------------------------------------------------
COMMON_ENV = Variable.get("pipeline_env", deserialize_json=True)

# ---------------------------------------------------------------------------
# Local Docker settings (used only when DEPLOY_TARGET == "local")
# ---------------------------------------------------------------------------
DOCKER_DEFAULTS = {
    "network_mode": "newslens_pipeline_default",
    "auto_remove": "success",
    "docker_url": "unix://var/run/docker.sock",
    "mount_tmp_dir": False,
}

# ---------------------------------------------------------------------------
# AWS ECS settings (used only when DEPLOY_TARGET == "aws")
# Read from Airflow Variables so they can be changed without code edits.
# ---------------------------------------------------------------------------
if DEPLOY_TARGET == "aws":
    ECS_CLUSTER = Variable.get("ecs_cluster", default_var="newslens-pipeline")
    ECS_SUBNETS = Variable.get("ecs_subnets", deserialize_json=True, default_var='[]')
    ECS_SECURITY_GROUPS = Variable.get("ecs_security_groups", deserialize_json=True, default_var='[]')
    ECR_REPO_PREFIX = Variable.get("ecr_repo_prefix", default_var="")
    ECS_TASK_ROLE = Variable.get("ecs_task_role", default_var="")
    ECS_EXECUTION_ROLE = Variable.get("ecs_execution_role", default_var="")

    ECS_NETWORK_CONFIG = {
        "awsvpcConfiguration": {
            "subnets": ECS_SUBNETS,
            "securityGroups": ECS_SECURITY_GROUPS,
            "assignPublicIp": "DISABLED",
        },
    }


# ---------------------------------------------------------------------------
# Helper: create the right operator based on deploy target
# ---------------------------------------------------------------------------
def _make_task(*, task_id: str, image: str, environment: dict, do_xcom_push: bool = True):
    if DEPLOY_TARGET == "aws":
        ecr_image = f"{ECR_REPO_PREFIX}/{image}" if ECR_REPO_PREFIX else image
        return EcsRunTaskOperator(
            task_id=task_id,
            cluster=ECS_CLUSTER,
            task_definition=f"newslens-{task_id}",
            launch_type="FARGATE",
            network_configuration=ECS_NETWORK_CONFIG,
            overrides={
                "containerOverrides": [{
                    "name": task_id,
                    "environment": [
                        {"name": k, "value": v} for k, v in environment.items()
                    ],
                }],
            },
            do_xcom_push=do_xcom_push,
            awslogs_group=f"/ecs/newslens-{task_id}",
            awslogs_stream_prefix="ecs",
        )
    else:
        return DockerOperator(
            task_id=task_id,
            image=image,
            environment=environment,
            do_xcom_push=do_xcom_push,
            **DOCKER_DEFAULTS,
        )


@dag(
    dag_id="news_event_pipeline",
    description="Scrape -> clean -> embed -> bias + cluster -> load",
    schedule="30 1 * * *",  # 7:00 AM IST (UTC+5:30) daily
    start_date=dt.datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["news", "nlp", "ml"],
)
def news_event_pipeline():

    scrape = _make_task(
        task_id="scrape",
        image="newslens/scraper:v1",
        environment={**COMMON_ENV, "RUN_DATE": "{{ ds }}"},
    )

    clean = _make_task(
        task_id="clean",
        image="newslens/cleaner:v1",
        environment={**COMMON_ENV, "INPUT_KEY": "{{ ti.xcom_pull(task_ids='scrape') }}"},
    )

    embed = _make_task(
        task_id="embed",
        image="newslens/embedder:v1",
        environment={**COMMON_ENV, "INPUT_KEY": "{{ ti.xcom_pull(task_ids='clean') }}"},
    )

    bias = _make_task(
        task_id="bias",
        image="newslens/bias-classifier-xgb:v1",
        environment={**COMMON_ENV, "INPUT_KEY": "{{ ti.xcom_pull(task_ids='embed') }}"},
    )

    cluster = _make_task(
        task_id="cluster",
        image="newslens/clustering:v1",
        environment={**COMMON_ENV, "INPUT_KEY": "{{ ti.xcom_pull(task_ids='embed') }}"},
    )

    load = _make_task(
        task_id="load",
        image="newslens/loader:v1",
        environment={
            **COMMON_ENV,
            "CLEAN_KEY": "{{ ti.xcom_pull(task_ids='clean') }}",
            "EMBEDDINGS_KEY": "{{ ti.xcom_pull(task_ids='embed') }}",
            "BIAS_KEY": "{{ ti.xcom_pull(task_ids='bias') }}",
            "CLUSTERS_KEY": "{{ ti.xcom_pull(task_ids='cluster') }}",
        },
        do_xcom_push=False,
    )

    # scrape -> clean -> embed -> [bias, cluster] -> load
    scrape >> clean >> embed >> [bias, cluster] >> load


news_event_pipeline()
