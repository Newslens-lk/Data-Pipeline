"""
Clustering stage — groups article embeddings into "events" with HDBSCAN.

Clusters within a rolling time window (CONFIG.cluster_window_hours) rather
than the whole historical corpus, for two reasons: (1) it keeps HDBSCAN's
runtime bounded as the corpus grows, and (2) "the same event" is inherently
a time-local concept — two similar articles six months apart are not the
same event, they're a recurring topic.

Cluster label -1 from HDBSCAN means "noise" (no event assigned) — these
articles are still stored, just without an event_id.
"""
from __future__ import annotations

import json
import logging
import uuid
from dataclasses import asdict, dataclass

import hdbscan
import numpy as np

from include.config import CONFIG

logger = logging.getLogger(__name__)


@dataclass
class ClusterAssignment:
    article_id: str
    event_id: str | None       # None if HDBSCAN labeled it noise
    cluster_label: int         # raw HDBSCAN label, -1 = noise
    cluster_probability: float  # HDBSCAN's soft membership probability


def cluster_embeddings(article_ids: list[str], embeddings: np.ndarray) -> list[ClusterAssignment]:
    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=CONFIG.hdbscan_min_cluster_size,
        min_samples=CONFIG.hdbscan_min_samples,
        metric=CONFIG.hdbscan_metric,
        prediction_data=False,
    )
    labels = clusterer.fit_predict(embeddings)
    probabilities = clusterer.probabilities_

    # Map raw integer labels -> stable UUID event ids (raw labels aren't meaningful
    # across runs; a UUID per run's cluster keeps downstream joins clean)
    label_to_event_id = {
        label: str(uuid.uuid4()) for label in set(labels) if label != -1
    }

    return [
        ClusterAssignment(
            article_id=aid,
            event_id=label_to_event_id.get(label),
            cluster_label=int(label),
            cluster_probability=float(prob),
        )
        for aid, label, prob in zip(article_ids, labels, probabilities)
    ]


def cluster_articles(embeddings_key: str) -> str:
    """Entry point called by the Airflow task.

    NOTE: in production you'll likely also pull recent (last `cluster_window_hours`)
    already-embedded articles from Postgres/vector DB here — not just this run's
    batch — so same-day articles scraped in different DAG runs still cluster
    together. Left as a TODO; wire it via include/db/vector_client.py.
    """
    from include.db.postgres_client import get_object_storage_hook

    hook = get_object_storage_hook()
    records = [json.loads(line) for line in hook.read_text(embeddings_key).splitlines()]

    article_ids = [r["article_id"] for r in records]
    embeddings = np.array([r["embedding"] for r in records])

    if len(article_ids) < CONFIG.hdbscan_min_cluster_size:
        logger.warning("Too few articles (%d) to cluster meaningfully; skipping.", len(article_ids))
        assignments = [
            ClusterAssignment(article_id=aid, event_id=None, cluster_label=-1, cluster_probability=0.0)
            for aid in article_ids
        ]
    else:
        assignments = cluster_embeddings(article_ids, embeddings)

    n_events = len({a.event_id for a in assignments if a.event_id})
    logger.info("Clustered %d articles into %d events", len(assignments), n_events)

    out_key = embeddings_key.replace("embeddings.ndjson", "clusters.ndjson")
    ndjson = "\n".join(json.dumps(asdict(a)) for a in assignments)
    hook.write_text(out_key, ndjson)
    return out_key
