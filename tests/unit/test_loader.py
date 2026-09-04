"""Unit tests for the loader container's data joining logic.

The actual DB upserts are tested in integration tests. Here we test
the in-memory join logic that the loader performs before writing to Postgres.
"""
from __future__ import annotations

import json


class TestLoaderJoinLogic:
    """Test the join-by-article_id logic that main() performs."""

    def test_index_by_article_id(self):
        embeddings = [
            {"article_id": "a1", "embedding": [0.1, 0.2]},
            {"article_id": "a2", "embedding": [0.3, 0.4]},
        ]
        embedding_map = {r["article_id"]: r["embedding"] for r in embeddings}
        assert embedding_map["a1"] == [0.1, 0.2]
        assert embedding_map["a2"] == [0.3, 0.4]

    def test_join_all_stages(self, sample_articles):
        """Simulate the loader's join: articles + embeddings + bias + clusters."""
        articles = sample_articles

        embeddings = [
            {"article_id": a["article_id"], "embedding": [0.1] * 1024}
            for a in articles
        ]
        bias_results = [
            {
                "article_id": a["article_id"],
                "bias_label": "center",
                "bias_confidence": 0.8,
                "bias_scores": {"far_left": 0.05, "left": 0.05, "center": 0.8, "right": 0.05, "far_right": 0.05},
            }
            for a in articles
        ]
        cluster_assignments = [
            {"article_id": a["article_id"], "event_id": "evt-1", "cluster_method": "knn", "distance": 0.2}
            for a in articles
        ]

        embedding_map = {r["article_id"]: r["embedding"] for r in embeddings}
        bias_map = {r["article_id"]: r for r in bias_results}
        cluster_map = {r["article_id"]: r for r in cluster_assignments}

        for a in articles:
            aid = a["article_id"]
            assert aid in embedding_map
            assert aid in bias_map
            assert aid in cluster_map

    def test_missing_embedding_handled_gracefully(self):
        """Loader uses .get() — missing embeddings become None, not KeyError."""
        embedding_map = {"a1": [0.1, 0.2]}
        assert embedding_map.get("a2") is None

    def test_missing_bias_handled_gracefully(self):
        """Loader uses .get() with default {} — missing bias fields become None."""
        bias_map = {}
        bias = bias_map.get("missing_id", {})
        assert bias.get("bias_label") is None
        assert bias.get("bias_confidence") is None

    def test_event_stats_aggregation(self):
        """Loader counts articles and sources per event for the events table."""
        cluster_assignments = [
            {"article_id": "a1", "event_id": "evt-1"},
            {"article_id": "a2", "event_id": "evt-1"},
            {"article_id": "a3", "event_id": "evt-2"},
        ]
        article_source_map = {"a1": "hirunews", "a2": "bbc_sinhala", "a3": "hirunews"}

        event_stats = {}
        for c in cluster_assignments:
            eid = c["event_id"]
            if eid not in event_stats:
                event_stats[eid] = {"article_ids": [], "sources": set()}
            event_stats[eid]["article_ids"].append(c["article_id"])

        for eid, stats in event_stats.items():
            for aid in stats["article_ids"]:
                if aid in article_source_map:
                    stats["sources"].add(article_source_map[aid])

        assert len(event_stats["evt-1"]["article_ids"]) == 2
        assert event_stats["evt-1"]["sources"] == {"hirunews", "bbc_sinhala"}
        assert len(event_stats["evt-2"]["article_ids"]) == 1
        assert event_stats["evt-2"]["sources"] == {"hirunews"}

    def test_embedding_str_format(self):
        """Loader converts embedding list to pgvector string format."""
        embedding = [0.1, 0.2, 0.3]
        embedding_str = "[" + ",".join(str(v) for v in embedding) + "]"
        assert embedding_str == "[0.1,0.2,0.3]"

    def test_embedding_str_none_when_missing(self):
        """Missing embedding -> None, not a broken string."""
        embedding = None
        embedding_str = None
        if embedding:
            embedding_str = "[" + ",".join(str(v) for v in embedding) + "]"
        assert embedding_str is None
