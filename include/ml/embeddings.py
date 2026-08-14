"""
Embedding generation stage — produces one vector per article for clustering
and later similarity search in the vector DB.
"""
from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from functools import lru_cache

import numpy as np

from include.config import CONFIG

logger = logging.getLogger(__name__)


@dataclass
class EmbeddingResult:
    article_id: str
    embedding: list[float]


@lru_cache(maxsize=1)
def _load_model():
    from sentence_transformers import SentenceTransformer

    logger.info("Loading embedding model %s", CONFIG.embedding_model_name)
    return SentenceTransformer(CONFIG.embedding_model_name)


def embed_texts(texts: list[str]) -> np.ndarray:
    model = _load_model()
    vectors = model.encode(
        texts,
        batch_size=CONFIG.embedding_batch_size,
        normalize_embeddings=True,  # cosine-ready; also required before HDBSCAN w/ euclidean metric
        show_progress_bar=False,
    )
    return np.asarray(vectors)


def embed_articles(clean_key: str) -> str:
    """Entry point called by the Airflow task."""
    from include.db.postgres_client import get_object_storage_hook

    hook = get_object_storage_hook()
    articles = [json.loads(line) for line in hook.read_text(clean_key).splitlines()]

    texts = [a["title"] + ". " + a["body"] for a in articles]
    vectors = embed_texts(texts)

    results = [
        EmbeddingResult(article_id=a["article_id"], embedding=vec.tolist())
        for a, vec in zip(articles, vectors)
    ]

    out_key = clean_key.replace("clean_articles.ndjson", "embeddings.ndjson")
    ndjson = "\n".join(json.dumps(asdict(r)) for r in results)
    hook.write_text(out_key, ndjson)

    logger.info("Embedded %d articles (dim=%d) -> %s", len(results), vectors.shape[1], out_key)
    return out_key
