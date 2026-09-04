"""
Integration test for the clustering container.

Full flow: seed embeddings in S3 -> mock DB (no existing clusters) ->
run main() -> verify cluster assignments NDJSON in S3.
Tests both KNN assignment and HDBSCAN fallback paths.
"""
from __future__ import annotations

import uuid
from unittest.mock import MagicMock, patch

import pytest

from tests.conftest import load_container
from tests.contracts.schemas import ClusterOutput
from tests.integration.conftest import BUCKET, read_ndjson, seed_ndjson


@pytest.fixture(scope="module")
def clustering():
    return load_container("clustering")


def _make_embeddings(embeddings_map: dict[str, list[float]]) -> list[dict]:
    """Build embeddings NDJSON records from {article_id: embedding} map."""
    return [
        {"article_id": aid, "embedding": emb}
        for aid, emb in embeddings_map.items()
    ]


class TestClusteringIntegration:
    def test_all_new_articles_get_assigned(self, clustering, s3_mock):
        """With empty DB, all articles go through HDBSCAN/single path."""
        embeddings = _make_embeddings({
            "aaa111bbb222ccc333ddd444": [0.0, 0.0],
            "bbb222ccc333ddd444eee555": [0.01, 0.0],
            "ccc333ddd444eee555fff666": [10.0, 10.0],
        })
        input_key = "embeddings/2026-01-01/embeddings.ndjson"
        expected_out_key = "clusters/2026-01-01/cluster_assignments.ndjson"
        seed_ndjson(s3_mock, input_key, embeddings)

        # Mock DB: no existing articles -> KNN returns nothing
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cur

        with patch.object(clustering, "get_s3_client", return_value=s3_mock), \
             patch.object(clustering, "get_db_connection", return_value=mock_conn), \
             patch.object(clustering, "INPUT_KEY", input_key), \
             patch.object(clustering, "STORAGE_BUCKET", BUCKET):
            clustering.main()

        output = read_ndjson(s3_mock, expected_out_key)

        # Every article gets an assignment
        assert len(output) == 3
        output_ids = {r["article_id"] for r in output}
        assert output_ids == {"aaa111bbb222ccc333ddd444", "bbb222ccc333ddd444eee555", "ccc333ddd444eee555fff666"}

        # Validate all records against schema
        for record in output:
            ClusterOutput(**record)
            assert record["cluster_method"] in ("hdbscan_new", "single")
            # Every article has a valid UUID event_id
            uuid.UUID(record["event_id"])

    def test_knn_assigns_to_existing_cluster(self, clustering, s3_mock):
        """When DB has close neighbors, articles are assigned via KNN."""
        embeddings = _make_embeddings({
            "aaa111bbb222ccc333ddd444": [0.1] * 10,
        })
        input_key = "embeddings/2026-01-01/embeddings.ndjson"
        seed_ndjson(s3_mock, input_key, embeddings)

        existing_event_id = str(uuid.uuid4())
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        # KNN returns 3 close neighbors all pointing to the same event
        mock_cur.fetchall.return_value = [
            (existing_event_id, 0.10),
            (existing_event_id, 0.12),
            (existing_event_id, 0.15),
        ]
        mock_conn.cursor.return_value = mock_cur

        with patch.object(clustering, "get_s3_client", return_value=s3_mock), \
             patch.object(clustering, "get_db_connection", return_value=mock_conn), \
             patch.object(clustering, "INPUT_KEY", input_key), \
             patch.object(clustering, "STORAGE_BUCKET", BUCKET):
            clustering.main()

        output = read_ndjson(s3_mock, "clusters/2026-01-01/cluster_assignments.ndjson")
        assert len(output) == 1

        record = output[0]
        ClusterOutput(**record)
        assert record["cluster_method"] == "knn"
        assert record["event_id"] == existing_event_id
        assert record["distance"] == pytest.approx(0.10)

    def test_mixed_knn_and_hdbscan(self, clustering, s3_mock):
        """Some articles match KNN, others fall through to HDBSCAN/single."""
        embeddings = _make_embeddings({
            "aaa111bbb222ccc333ddd444": [0.1] * 10,  # will match KNN
            "bbb222ccc333ddd444eee555": [5.0] * 10,  # no match -> single
        })
        input_key = "embeddings/2026-01-01/embeddings.ndjson"
        seed_ndjson(s3_mock, input_key, embeddings)

        existing_event_id = str(uuid.uuid4())
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        # First call: close neighbors (KNN match)
        # Second call: no neighbors (falls to HDBSCAN/single)
        mock_cur.fetchall.side_effect = [
            [(existing_event_id, 0.10), (existing_event_id, 0.12), (existing_event_id, 0.15)],
            [],
        ]
        mock_conn.cursor.return_value = mock_cur

        with patch.object(clustering, "get_s3_client", return_value=s3_mock), \
             patch.object(clustering, "get_db_connection", return_value=mock_conn), \
             patch.object(clustering, "INPUT_KEY", input_key), \
             patch.object(clustering, "STORAGE_BUCKET", BUCKET):
            clustering.main()

        output = read_ndjson(s3_mock, "clusters/2026-01-01/cluster_assignments.ndjson")
        assert len(output) == 2

        methods = {r["article_id"]: r["cluster_method"] for r in output}
        assert methods["aaa111bbb222ccc333ddd444"] == "knn"
        assert methods["bbb222ccc333ddd444eee555"] == "single"
