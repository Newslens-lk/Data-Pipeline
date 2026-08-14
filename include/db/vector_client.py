"""
Optional: use this instead of / alongside pgvector if you outgrow Postgres
for similarity search (e.g. need approximate nearest-neighbor at large scale,
or want managed hybrid search). Not wired into the DAG by default since the
current plan is Postgres + pgvector — swap load_final_results in
postgres_client.py to also call upsert_vectors() below if you add this.

Example shown for Qdrant; swap for Pinecone/Weaviate similarly — same
upsert/query interface shape.
"""
from __future__ import annotations

import logging

from include.config import CONFIG

logger = logging.getLogger(__name__)


def _client():
    from qdrant_client import QdrantClient
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection(CONFIG.vector_db_conn_id)
    return QdrantClient(url=conn.host, api_key=conn.password)


def ensure_collection(collection_name: str = "news_articles") -> None:
    from qdrant_client.models import Distance, VectorParams

    client = _client()
    if not client.collection_exists(collection_name):
        client.create_collection(
            collection_name=collection_name,
            vectors_config=VectorParams(size=CONFIG.embedding_dim, distance=Distance.COSINE),
        )


def upsert_vectors(article_ids: list[str], embeddings: list[list[float]], payloads: list[dict],
                    collection_name: str = "news_articles") -> None:
    from qdrant_client.models import PointStruct

    client = _client()
    points = [
        PointStruct(id=aid, vector=emb, payload=payload)
        for aid, emb, payload in zip(article_ids, embeddings, payloads)
    ]
    client.upsert(collection_name=collection_name, points=points)
    logger.info("Upserted %d vectors into %s", len(points), collection_name)
