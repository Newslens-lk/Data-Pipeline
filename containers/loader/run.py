"""
Loader container entry point.

Final pipeline stage. Reads all intermediate NDJSON outputs from MinIO
(cleaned articles, embeddings, bias results, cluster assignments) and
writes everything to the news-db database in a single transaction.

Environment variables:
    CLEAN_KEY             - MinIO key for cleaned articles NDJSON
    EMBEDDINGS_KEY        - MinIO key for embeddings NDJSON
    BIAS_KEY              - MinIO key for bias results NDJSON
    CLUSTERS_KEY          - MinIO key for cluster assignments NDJSON
    STORAGE_ENDPOINT      - S3/MinIO endpoint URL
    STORAGE_BUCKET        - bucket name
    AWS_ACCESS_KEY_ID     - S3/MinIO access key
    AWS_SECRET_ACCESS_KEY - S3/MinIO secret key
    DB_HOST               - news-db host (default: news-db)
    DB_PORT               - news-db port (default: 5432)
    DB_NAME               - database name (default: news_pipeline)
    DB_USER               - database user (default: news)
    DB_PASSWORD           - database password (default: news)
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

import boto3
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CLEAN_KEY = os.environ["CLEAN_KEY"]
EMBEDDINGS_KEY = os.environ["EMBEDDINGS_KEY"]
BIAS_KEY = os.environ["BIAS_KEY"]
CLUSTERS_KEY = os.environ["CLUSTERS_KEY"]
STORAGE_ENDPOINT = os.environ["STORAGE_ENDPOINT"]
STORAGE_BUCKET = os.environ["STORAGE_BUCKET"]

DB_HOST = os.environ.get("DB_HOST", "news-db")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "news_pipeline")
DB_USER = os.environ.get("DB_USER", "news")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "news")


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=STORAGE_ENDPOINT,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )


def read_ndjson(s3, key: str) -> list[dict]:
    obj = s3.get_object(Bucket=STORAGE_BUCKET, Key=key)
    lines = obj["Body"].read().decode("utf-8").strip().splitlines()
    return [json.loads(line) for line in lines]


def main():
    s3 = get_s3_client()

    # Read all intermediate outputs
    articles = read_ndjson(s3, CLEAN_KEY)
    embeddings = read_ndjson(s3, EMBEDDINGS_KEY)
    bias_results = read_ndjson(s3, BIAS_KEY)
    cluster_assignments = read_ndjson(s3, CLUSTERS_KEY)

    logger.info("Read %d articles, %d embeddings, %d bias results, %d cluster assignments",
                len(articles), len(embeddings), len(bias_results), len(cluster_assignments))

    # Index by article_id for joining
    embedding_map = {r["article_id"]: r["embedding"] for r in embeddings}
    bias_map = {r["article_id"]: r for r in bias_results}
    cluster_map = {r["article_id"]: r for r in cluster_assignments}

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
    )
    cur = conn.cursor()

    # 1. Insert sources (deduplicated)
    sources = set(a["source"] for a in articles)
    source_rows = [(s, "html") for s in sources]
    execute_values(cur, """
        INSERT INTO sources (source_name, source_type)
        VALUES %s
        ON CONFLICT (source_name) DO NOTHING
    """, source_rows)
    logger.info("Upserted %d sources", len(sources))

    # 2. Collect all event_ids and insert events
    event_ids = set()
    for c in cluster_assignments:
        event_ids.add(c["event_id"])

    # Count articles and sources per event
    event_stats = {}
    for c in cluster_assignments:
        eid = c["event_id"]
        if eid not in event_stats:
            event_stats[eid] = {"article_ids": [], "sources": set()}
        event_stats[eid]["article_ids"].append(c["article_id"])

    # Get source names for each event's articles
    article_source_map = {a["article_id"]: a["source"] for a in articles}
    for eid, stats in event_stats.items():
        for aid in stats["article_ids"]:
            if aid in article_source_map:
                stats["sources"].add(article_source_map[aid])

    event_rows = []
    for eid, stats in event_stats.items():
        event_rows.append((
            eid,
            len(stats["article_ids"]),
            len(stats["sources"]),
        ))

    execute_values(cur, """
        INSERT INTO events (event_id, article_count, source_count)
        VALUES %s
        ON CONFLICT (event_id) DO UPDATE SET
            article_count = events.article_count + EXCLUDED.article_count,
            source_count = GREATEST(events.source_count, EXCLUDED.source_count)
    """, event_rows)
    logger.info("Upserted %d events", len(event_rows))

    # 3. Insert articles with all data joined
    now = datetime.now(timezone.utc).isoformat()
    article_rows = []
    for a in articles:
        aid = a["article_id"]
        embedding = embedding_map.get(aid)
        bias = bias_map.get(aid, {})
        cluster = cluster_map.get(aid, {})

        embedding_str = None
        if embedding:
            embedding_str = "[" + ",".join(str(v) for v in embedding) + "]"

        article_rows.append((
            aid,
            a["source"],
            a["url"],
            a["title"],
            a["body"],
            a.get("language", "si"),
            a.get("published_at"),
            a.get("scraped_at", now),
            bias.get("bias_label"),
            bias.get("bias_confidence"),
            json.dumps(bias["bias_scores"]) if "bias_scores" in bias else None,
            embedding_str,
            cluster.get("event_id"),
        ))

    execute_values(cur, """
        INSERT INTO articles (
            article_id, source_name, url, title, body, language,
            published_at, scraped_at, bias_label, bias_confidence,
            bias_scores, embedding, event_id
        )
        VALUES %s
        ON CONFLICT (article_id) DO UPDATE SET
            bias_label = COALESCE(EXCLUDED.bias_label, articles.bias_label),
            bias_confidence = COALESCE(EXCLUDED.bias_confidence, articles.bias_confidence),
            bias_scores = COALESCE(EXCLUDED.bias_scores, articles.bias_scores),
            embedding = COALESCE(EXCLUDED.embedding, articles.embedding),
            event_id = COALESCE(EXCLUDED.event_id, articles.event_id)
    """, article_rows)
    logger.info("Upserted %d articles", len(article_rows))

    # 4. Update vector index
    cur.execute("ANALYZE articles")

    conn.commit()
    cur.close()
    conn.close()

    logger.info("Database load complete")
    print("done")


if __name__ == "__main__":
    main()
