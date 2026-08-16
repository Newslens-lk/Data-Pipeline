"""
Clustering container entry point.

Reads new article embeddings from MinIO, queries the database for nearest
neighbors, and assigns each article to an existing cluster or creates a new one.
Uses KNN with pgvector L2 distance for incremental cluster assignment.

After individual assignment, unassigned articles are batch-clustered with
HDBSCAN to discover new multi-article events.

Writes cluster assignments NDJSON back to MinIO and updates the database.
Prints the output key to stdout for Airflow to capture via XCom.

Environment variables:
    INPUT_KEY             - MinIO key for embeddings NDJSON
    STORAGE_ENDPOINT      - S3/MinIO endpoint URL
    STORAGE_BUCKET        - bucket name
    AWS_ACCESS_KEY_ID     - S3/MinIO access key
    AWS_SECRET_ACCESS_KEY - S3/MinIO secret key
    DB_HOST               - news-db host (default: news-db)
    DB_PORT               - news-db port (default: 5432)
    DB_NAME               - database name (default: news_pipeline)
    DB_USER               - database user (default: news)
    DB_PASSWORD           - database password (default: news)
    DISTANCE_THRESHOLD    - max L2 distance for same-cluster assignment (default: 0.50)
    K_NEIGHBORS           - number of nearest neighbors to check (default: 5)
    MAJORITY_RATIO        - fraction of K that must agree on a cluster (default: 0.6)
"""
from __future__ import annotations

import json
import logging
import os
import uuid
from collections import Counter

import boto3
import hdbscan
import numpy as np
import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

INPUT_KEY = os.environ["INPUT_KEY"]
STORAGE_ENDPOINT = os.environ["STORAGE_ENDPOINT"]
STORAGE_BUCKET = os.environ["STORAGE_BUCKET"]

DB_HOST = os.environ.get("DB_HOST", "news-db")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "news_pipeline")
DB_USER = os.environ.get("DB_USER", "news")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "news")

DISTANCE_THRESHOLD = float(os.environ.get("DISTANCE_THRESHOLD", "0.50"))
K_NEIGHBORS = int(os.environ.get("K_NEIGHBORS", "5"))
MAJORITY_RATIO = float(os.environ.get("MAJORITY_RATIO", "0.6"))


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=STORAGE_ENDPOINT,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )


def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
    )


def find_nearest_cluster(cur, embedding: list[float]) -> tuple[str | None, float]:
    """
    Query pgvector for K nearest neighbors and determine cluster by majority vote.
    Returns (event_id, distance) or (None, inf) if no match.
    """
    embedding_str = "[" + ",".join(str(v) for v in embedding) + "]"

    cur.execute("""
        SELECT event_id, embedding <-> %s::vector AS distance
        FROM articles
        WHERE embedding IS NOT NULL AND event_id IS NOT NULL
        ORDER BY embedding <-> %s::vector
        LIMIT %s
    """, (embedding_str, embedding_str, K_NEIGHBORS))

    rows = cur.fetchall()
    if not rows:
        return None, float("inf")

    # Filter neighbors within distance threshold
    close_neighbors = [(event_id, dist) for event_id, dist in rows if dist <= DISTANCE_THRESHOLD]

    if not close_neighbors:
        return None, float("inf")

    # Majority vote on event_id
    event_counts = Counter(event_id for event_id, _ in close_neighbors)
    top_event, top_count = event_counts.most_common(1)[0]

    # Check if majority agrees
    if top_count / len(close_neighbors) >= MAJORITY_RATIO:
        nearest_dist = min(dist for event_id, dist in close_neighbors if event_id == top_event)
        return top_event, nearest_dist

    return None, float("inf")


def batch_cluster_unassigned(unassigned: list[dict]) -> dict[int, str]:
    """
    Run HDBSCAN on unassigned articles to discover new multi-article events.
    Returns mapping of article index -> event_id.
    """
    if len(unassigned) < 2:
        return {}

    embeddings = np.array([a["embedding"] for a in unassigned])

    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=2,
        min_samples=1,
        metric="euclidean",
        cluster_selection_method="eom",
    )
    labels = clusterer.fit_predict(embeddings)

    # Create event UUIDs for each new cluster
    cluster_event_map = {}
    index_event_map = {}

    for i, label in enumerate(labels):
        if label == -1:
            continue
        if label not in cluster_event_map:
            cluster_event_map[label] = str(uuid.uuid4())
        index_event_map[i] = cluster_event_map[label]

    return index_event_map


def main():
    s3 = get_s3_client()
    conn = get_db_connection()
    cur = conn.cursor()

    # Read embeddings
    obj = s3.get_object(Bucket=STORAGE_BUCKET, Key=INPUT_KEY)
    lines = obj["Body"].read().decode("utf-8").strip().splitlines()
    records = [json.loads(line) for line in lines]
    logger.info("Read %d embeddings from s3://%s/%s", len(records), STORAGE_BUCKET, INPUT_KEY)

    results = []
    unassigned = []
    unassigned_indices = []

    # Phase 1: Try to assign each article to an existing cluster via KNN
    for i, record in enumerate(records):
        event_id, distance = find_nearest_cluster(cur, record["embedding"])

        if event_id is not None:
            results.append({
                "article_id": record["article_id"],
                "event_id": event_id,
                "cluster_method": "knn",
                "distance": distance,
            })
            logger.debug("Assigned %s to existing cluster %s (dist=%.4f)",
                         record["article_id"], event_id, distance)
        else:
            unassigned.append(record)
            unassigned_indices.append(i)

    logger.info("KNN assigned %d/%d articles to existing clusters",
                len(results), len(records))

    # Phase 2: Batch-cluster unassigned articles with HDBSCAN
    if unassigned:
        new_clusters = batch_cluster_unassigned(unassigned)

        # Articles that formed new clusters
        new_event_ids = set()
        for idx, event_id in new_clusters.items():
            new_event_ids.add(event_id)
            results.append({
                "article_id": unassigned[idx]["article_id"],
                "event_id": event_id,
                "cluster_method": "hdbscan_new",
                "distance": 0.0,
            })

        # Insert new events into the database
        for event_id in new_event_ids:
            cluster_articles = [r for r in results if r["event_id"] == event_id]
            cur.execute("""
                INSERT INTO events (event_id, article_count, source_count)
                VALUES (%s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
            """, (event_id, len(cluster_articles), len(cluster_articles)))

        # Remaining unassigned become single-article events
        assigned_unassigned = set(new_clusters.keys())
        for idx, record in enumerate(unassigned):
            if idx not in assigned_unassigned:
                single_event_id = str(uuid.uuid4())
                results.append({
                    "article_id": record["article_id"],
                    "event_id": single_event_id,
                    "cluster_method": "single",
                    "distance": 0.0,
                })
                cur.execute("""
                    INSERT INTO events (event_id, article_count, source_count)
                    VALUES (%s, 1, 1)
                    ON CONFLICT (event_id) DO NOTHING
                """, (single_event_id,))

        logger.info("HDBSCAN formed %d new clusters from %d unassigned articles, %d remain as singles",
                     len(new_event_ids), len(unassigned),
                     len(unassigned) - len(assigned_unassigned))

    # Phase 3: Update article rows in the database
    for r in results:
        cur.execute("""
            UPDATE articles SET event_id = %s
            WHERE article_id = %s
        """, (r["event_id"], r["article_id"]))

    conn.commit()
    cur.close()
    conn.close()
    logger.info("Updated %d article rows in database", len(results))

    # Write output NDJSON
    ndjson = "\n".join(json.dumps(r) for r in results)
    date_part = INPUT_KEY.split("/")[1]
    out_key = f"clusters/{date_part}/cluster_assignments.ndjson"
    s3.put_object(Bucket=STORAGE_BUCKET, Key=out_key, Body=ndjson.encode("utf-8"))

    logger.info("Wrote %d assignments to s3://%s/%s", len(results), STORAGE_BUCKET, out_key)
    print(out_key)


if __name__ == "__main__":
    main()
