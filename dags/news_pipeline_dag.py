"""
News pipeline DAG — DockerOperator version.

Each task launches a standalone Docker container on the host machine.
Containers communicate through MinIO (NDJSON files), not directly.
The storage key each container prints to stdout becomes its XCom value,
which downstream tasks receive as environment variables.

Pipeline:  scrape -> clean -> embed -> [bias, cluster] -> load
                                        (parallel)

Schedule: every 4 hours. Set catchup=False so Airflow doesn't try to
backfill every missed 4-hour slot since start_date.
"""

from __future__ import annotations

import datetime as dt

from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator

# ---------------------------------------------------------------------------
# Default args applied to every task unless overridden.
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data-eng",
    # If a container fails, retry up to 2 times with exponential backoff.
    # First retry after 5 min, second after 10 min (capped at 30 min).
    "retries": 2,
    "retry_delay": dt.timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": dt.timedelta(minutes=30),
}

# ---------------------------------------------------------------------------
# Shared environment variables — loaded from an Airflow Variable called
# "pipeline_env" (a JSON dict stored in Airflow's metadata DB, encrypted
# with the Fernet key).
#
# This keeps secrets (MinIO keys, DB passwords) out of version-controlled
# code. To view or update, use:
#   airflow variables get pipeline_env
#   airflow variables set pipeline_env '{"KEY": "value", ...}'
# or edit via the Airflow UI at Admin -> Variables.
# ---------------------------------------------------------------------------
COMMON_ENV = Variable.get("pipeline_env", deserialize_json=True)

# ---------------------------------------------------------------------------
# Shared DockerOperator settings — avoids repeating the same lines
# in every task definition.
# ---------------------------------------------------------------------------
#
# network_mode: containers must join the compose network so they can
#   reach "minio:9000" and "news-db:5432" by hostname.
#
# auto_remove: delete the container after it exits so we don't
#   accumulate stopped containers on the host.
#
# docker_url: tells DockerOperator where the Docker daemon is.
#   The scheduler container has /var/run/docker.sock mounted,
#   so this is the standard Unix socket path.
#
# mount_tmp_dir: False because we don't need Airflow to mount a
#   temp directory into our containers — they read/write via MinIO.
#
DOCKER_DEFAULTS = {
    "network_mode": "newslens_pipeline_default",
    "auto_remove": "success",
    "docker_url": "unix://var/run/docker.sock",
    "mount_tmp_dir": False,
}


@dag(
    dag_id="news_event_pipeline",
    description="Scrape -> clean -> embed -> bias + cluster -> load",
    # Run every 4 hours: at minute 0 of hours 0, 4, 8, 12, 16, 20.
    schedule="0 */4 * * *",
    # start_date is required by Airflow but with catchup=False it just
    # marks the earliest possible run — Airflow won't backfill old runs.
    start_date=dt.datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    # Only one run at a time — prevents two runs writing to the same
    # date folder in MinIO or creating duplicate clusters.
    max_active_runs=1,
    tags=["news", "nlp", "ml"],
)
def news_event_pipeline():

    # -------------------------------------------------------------------
    # Task 1: SCRAPE
    # -------------------------------------------------------------------
    # Launches the scraper container. It crawls news sources, writes
    # articles to MinIO at "raw/{date}/articles.ndjson", and prints
    # that key to stdout.
    #
    # RUN_DATE is passed via environment so the scraper knows which
    # date folder to write to. {{ ds }} is an Airflow template that
    # resolves to the logical run date (e.g., "2026-08-17").
    #
    # do_xcom_push=True tells DockerOperator to capture the last line
    # of stdout and store it as this task's XCom return value.
    # -------------------------------------------------------------------
    scrape = DockerOperator(
        task_id="scrape",
        image="newslens/scraper:v1",
        # {**COMMON_ENV, ...} merges the shared env vars with task-specific
        # ones. If a key appears in both, the task-specific value wins.
        environment={**COMMON_ENV, "RUN_DATE": "{{ ds }}"},
        do_xcom_push=True,
        **DOCKER_DEFAULTS,
    )

    # -------------------------------------------------------------------
    # Task 2: CLEAN
    # -------------------------------------------------------------------
    # Takes the scraper's output key from XCom and passes it as
    # INPUT_KEY. The cleaner reads that NDJSON from MinIO, normalizes
    # text, removes duplicates, and writes the result to
    # "cleaned/{date}/articles.ndjson".
    #
    # ti.xcom_pull(task_ids='scrape') retrieves the string that the
    # scrape task pushed — Airflow resolves this Jinja template
    # before the container starts.
    # -------------------------------------------------------------------
    clean = DockerOperator(
        task_id="clean",
        image="newslens/cleaner:v1",
        environment={**COMMON_ENV, "INPUT_KEY": "{{ ti.xcom_pull(task_ids='scrape') }}"},
        do_xcom_push=True,
        **DOCKER_DEFAULTS,
    )

    # -------------------------------------------------------------------
    # Task 3: EMBED
    # -------------------------------------------------------------------
    # Reads cleaned articles, generates 1024-dim embeddings using
    # E5-large (via Modal remote GPU when USE_MODAL=true in .env),
    # writes to "embeddings/{date}/embeddings.ndjson".
    #
    # Bias classification and clustering both need embeddings,
    # so this must finish before either of them can start.
    # -------------------------------------------------------------------
    embed = DockerOperator(
        task_id="embed",
        image="newslens/embedder:v1",
        environment={**COMMON_ENV, "INPUT_KEY": "{{ ti.xcom_pull(task_ids='clean') }}"},
        do_xcom_push=True,
        **DOCKER_DEFAULTS,
    )

    # -------------------------------------------------------------------
    # Task 4a: BIAS CLASSIFICATION (runs in parallel with clustering)
    # -------------------------------------------------------------------
    # XGBoost classifier that reads embeddings from MinIO, predicts
    # bias labels, and writes to "bias/{date}/bias_results.ndjson".
    #
    # The model file location (MODEL_KEY) comes from .env, so we
    # don't need to set it here.
    # -------------------------------------------------------------------
    bias = DockerOperator(
        task_id="bias",
        image="newslens/bias-classifier-xgb:v1",
        environment={**COMMON_ENV, "INPUT_KEY": "{{ ti.xcom_pull(task_ids='embed') }}"},
        do_xcom_push=True,
        **DOCKER_DEFAULTS,
    )

    # -------------------------------------------------------------------
    # Task 4b: CLUSTER (runs in parallel with bias classification)
    # -------------------------------------------------------------------
    # Reads embeddings, does KNN against existing DB embeddings to
    # find matching events, then runs HDBSCAN on leftovers to form
    # new clusters. Writes to "clusters/{date}/cluster_assignments.ndjson".
    #
    # DB credentials for the KNN lookup come from .env (DB_HOST, etc.
    # default to news-db:5432 if not set).
    # -------------------------------------------------------------------
    cluster = DockerOperator(
        task_id="cluster",
        image="newslens/clustering:v1",
        environment={**COMMON_ENV, "INPUT_KEY": "{{ ti.xcom_pull(task_ids='embed') }}"},
        do_xcom_push=True,
        **DOCKER_DEFAULTS,
    )

    # -------------------------------------------------------------------
    # Task 5: LOAD
    # -------------------------------------------------------------------
    # Final stage. Reads all intermediate NDJSON files from MinIO
    # and writes everything to the news-db in a single transaction.
    #
    # Unlike other tasks that take a single INPUT_KEY, the loader
    # needs four separate keys — one for each upstream output.
    # Each is pulled from the corresponding task's XCom.
    #
    # Prints "done" to stdout when finished.
    # -------------------------------------------------------------------
    load = DockerOperator(
        task_id="load",
        image="newslens/loader:v1",
        environment={
            **COMMON_ENV,
            "CLEAN_KEY": "{{ ti.xcom_pull(task_ids='clean') }}",
            "EMBEDDINGS_KEY": "{{ ti.xcom_pull(task_ids='embed') }}",
            "BIAS_KEY": "{{ ti.xcom_pull(task_ids='bias') }}",
            "CLUSTERS_KEY": "{{ ti.xcom_pull(task_ids='cluster') }}",
        },
        do_xcom_push=False,  # nothing downstream needs loader's output
        **DOCKER_DEFAULTS,
    )

    # -------------------------------------------------------------------
    # Task dependencies — defines execution order.
    # -------------------------------------------------------------------
    # >> means "must finish before". Reading left to right:
    #
    #   scrape finishes -> clean starts
    #   clean finishes  -> embed starts
    #   embed finishes  -> bias AND cluster start (in parallel)
    #   both bias AND cluster finish -> load starts
    #
    # Visual:
    #
    #   scrape -> clean -> embed -> bias    \
    #                           -> cluster  -> load
    # -------------------------------------------------------------------
    scrape >> clean >> embed >> [bias, cluster] >> load


news_event_pipeline()
