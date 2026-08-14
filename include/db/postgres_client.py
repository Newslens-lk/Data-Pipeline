"""
Thin wrappers around Airflow's connection/hook system so include/ modules
don't need to import Airflow directly (keeps them unit-testable without a
running Airflow instance — see tests/).
"""
from __future__ import annotations

import json
import logging

from include.config import CONFIG

logger = logging.getLogger(__name__)


def get_object_storage_hook():
    """
    Returns a small wrapper exposing .read_text(key) / .write_text(key, text),
    backed by whichever object storage Airflow connection is configured
    (S3Hook / GCSHook / WasbHook depending on your cloud). Centralizing this
    means scraper.py / cleaning.py / ml modules never care which cloud you're on.
    """
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection(CONFIG.object_storage_conn_id)
    conn_type = conn.conn_type

    if conn_type == "aws":
        return _S3TextHook()
    if conn_type == "google_cloud_platform":
        return _GCSTextHook()
    if conn_type == "wasb":
        return _AzureBlobTextHook()
    raise ValueError(f"Unsupported object storage connection type: {conn_type}")


class _S3TextHook:
    def __init__(self):
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        self.hook = S3Hook(aws_conn_id=CONFIG.object_storage_conn_id)
        self.bucket = CONFIG.env and f"news-pipeline-{CONFIG.env}"  # TODO: move to CONFIG explicitly

    def read_text(self, key: str) -> str:
        return self.hook.read_key(key=key, bucket_name=self.bucket)

    def write_text(self, key: str, text: str) -> None:
        self.hook.load_string(string_data=text, key=key, bucket_name=self.bucket, replace=True)


class _GCSTextHook:
    def __init__(self):
        from airflow.providers.google.cloud.hooks.gcs import GCSHook

        self.hook = GCSHook(gcp_conn_id=CONFIG.object_storage_conn_id)
        self.bucket = f"news-pipeline-{CONFIG.env}"

    def read_text(self, key: str) -> str:
        return self.hook.download(bucket_name=self.bucket, object_name=key).decode("utf-8")

    def write_text(self, key: str, text: str) -> None:
        self.hook.upload(bucket_name=self.bucket, object_name=key, data=text.encode("utf-8"))


class _AzureBlobTextHook:
    def __init__(self):
        from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

        self.hook = WasbHook(wasb_conn_id=CONFIG.object_storage_conn_id)
        self.container = f"news-pipeline-{CONFIG.env}"

    def read_text(self, key: str) -> str:
        return self.hook.read_file(container_name=self.container, blob_name=key)

    def write_text(self, key: str, text: str) -> None:
        self.hook.load_string(string_data=text, container_name=self.container, blob_name=key, overwrite=True)


def load_final_results(clean_key: str, bias_key: str, embeddings_key: str,
                        clusters_key: str, summaries_key: str, topics_key: str) -> None:
    """
    Entry point called by the Airflow load task. Joins all intermediate
    NDJSON outputs on article_id / event_id and upserts into Postgres.
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = get_object_storage_hook()

    def _load(key: str, index_field: str) -> dict:
        return {r[index_field]: r for r in (json.loads(l) for l in hook.read_text(key).splitlines())}

    articles = _load(clean_key, "article_id")
    bias = _load(bias_key, "article_id")
    embeddings = _load(embeddings_key, "article_id")
    clusters = _load(clusters_key, "article_id")
    summaries = _load(summaries_key, "event_id")
    topics = _load(topics_key, "event_id")

    pg = PostgresHook(postgres_conn_id=CONFIG.postgres_conn_id)

    event_rows = [
        (
            event_id,
            summaries[event_id]["summary"],
            topics.get(event_id, {}).get("topic", "other"),
            summaries[event_id]["article_count"],
            summaries[event_id]["source_count"],
        )
        for event_id in summaries
    ]
    pg.insert_rows(
        table="events",
        rows=event_rows,
        target_fields=["event_id", "summary", "topic", "article_count", "source_count"],
        replace=True,
        replace_index="event_id",
    )

    article_rows = []
    for article_id, a in articles.items():
        b = bias.get(article_id, {})
        e = embeddings.get(article_id, {})
        c = clusters.get(article_id, {})
        article_rows.append((
            article_id, a["source"], a["url"], a["title"], a["body"], a["language"],
            a.get("published_at"), a["scraped_at"],
            b.get("bias_label"), b.get("bias_confidence"), json.dumps(b.get("bias_scores")) if b else None,
            e.get("embedding"), c.get("event_id"),
        ))
    pg.insert_rows(
        table="articles",
        rows=article_rows,
        target_fields=[
            "article_id", "source_name", "url", "title", "body", "language",
            "published_at", "scraped_at", "bias_label", "bias_confidence", "bias_scores",
            "embedding", "event_id",
        ],
        replace=True,
        replace_index="article_id",
    )

    logger.info("Loaded %d articles and %d events into Postgres", len(article_rows), len(event_rows))
