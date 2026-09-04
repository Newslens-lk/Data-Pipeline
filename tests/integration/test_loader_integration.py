"""
Integration test for the loader container.

Full flow: seed all 4 intermediate NDJSON files in S3 -> mock Postgres ->
run main() -> verify correct SQL calls with joined data.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from tests.conftest import load_container
from tests.integration.conftest import BUCKET, seed_ndjson


@pytest.fixture(scope="module")
def loader():
    return load_container("loader")


def _seed_all_stages(s3, cleaned_articles):
    """Seed S3 with all 4 intermediate NDJSON files the loader needs."""
    article_ids = [a["article_id"] for a in cleaned_articles]

    clean_key = "cleaned/2026-01-01/articles.ndjson"
    emb_key = "embeddings/2026-01-01/embeddings.ndjson"
    bias_key = "bias/2026-01-01/bias_results.ndjson"
    cluster_key = "clusters/2026-01-01/cluster_assignments.ndjson"

    seed_ndjson(s3, clean_key, cleaned_articles)

    seed_ndjson(s3, emb_key, [
        {"article_id": aid, "embedding": [0.1] * 1024}
        for aid in article_ids
    ])

    seed_ndjson(s3, bias_key, [
        {
            "article_id": aid,
            "bias_label": "center",
            "bias_confidence": 0.80,
            "bias_scores": {"far_left": 0.05, "left": 0.05, "center": 0.80, "right": 0.05, "far_right": 0.05},
        }
        for aid in article_ids
    ])

    seed_ndjson(s3, cluster_key, [
        {"article_id": aid, "event_id": "evt-001", "cluster_method": "knn", "distance": 0.2}
        for aid in article_ids
    ])

    return clean_key, emb_key, bias_key, cluster_key


def _run_loader_with_capture(loader, s3_mock, cleaned_articles):
    """Run loader.main() with mocked S3 + DB, capturing execute_values calls."""
    clean_key, emb_key, bias_key, cluster_key = _seed_all_stages(s3_mock, cleaned_articles)

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value = mock_cur

    captured_calls = []

    def capture_execute_values(cur, sql, rows, *args, **kwargs):
        captured_calls.append({"sql": sql, "rows": list(rows)})

    # Patch execute_values on the loader module (it did `from psycopg2.extras import execute_values`)
    with patch.object(loader, "get_s3_client", return_value=s3_mock), \
         patch.object(loader, "CLEAN_KEY", clean_key), \
         patch.object(loader, "EMBEDDINGS_KEY", emb_key), \
         patch.object(loader, "BIAS_KEY", bias_key), \
         patch.object(loader, "CLUSTERS_KEY", cluster_key), \
         patch.object(loader, "STORAGE_BUCKET", BUCKET), \
         patch.object(loader, "execute_values", capture_execute_values), \
         patch("psycopg2.connect", return_value=mock_conn):
        loader.main()

    return captured_calls, mock_conn, mock_cur


class TestLoaderIntegration:
    def test_full_flow_reads_and_joins(self, loader, s3_mock, cleaned_articles):
        """Loader reads all 4 NDJSON files and calls DB with joined rows."""
        captured_calls, mock_conn, mock_cur = _run_loader_with_capture(
            loader, s3_mock, cleaned_articles
        )

        # 3 execute_values calls: sources, events, articles
        assert len(captured_calls) == 3
        assert "sources" in captured_calls[0]["sql"]
        assert "events" in captured_calls[1]["sql"]
        assert "articles" in captured_calls[2]["sql"]

        # ANALYZE and commit called
        mock_cur.execute.assert_called_once()
        mock_conn.commit.assert_called_once()

    def test_sources_deduplicated(self, loader, s3_mock, cleaned_articles):
        """Loader should insert unique sources only."""
        captured_calls, _, _ = _run_loader_with_capture(
            loader, s3_mock, cleaned_articles
        )

        sources_call = captured_calls[0]
        assert "sources" in sources_call["sql"]
        source_names = {row[0] for row in sources_call["rows"]}
        # cleaned_articles has 2 unique sources: hirunews and bbc_sinhala
        assert source_names == {"hirunews", "bbc_sinhala"}

    def test_events_created_from_clusters(self, loader, s3_mock, cleaned_articles):
        """Loader creates events from cluster assignments."""
        captured_calls, _, _ = _run_loader_with_capture(
            loader, s3_mock, cleaned_articles
        )

        events_call = captured_calls[1]
        assert "events" in events_call["sql"]
        # All 3 articles share evt-001
        event_rows = events_call["rows"]
        assert len(event_rows) == 1
        assert event_rows[0][0] == "evt-001"  # event_id
        assert event_rows[0][1] == 3           # article_count
        assert event_rows[0][2] == 2           # source_count (hirunews + bbc_sinhala)

    def test_articles_include_all_joined_fields(self, loader, s3_mock, cleaned_articles):
        """Each article row should have embedding, bias, and cluster data."""
        captured_calls, _, _ = _run_loader_with_capture(
            loader, s3_mock, cleaned_articles
        )

        articles_call = captured_calls[2]
        assert "articles" in articles_call["sql"]
        assert len(articles_call["rows"]) == 3

        for row in articles_call["rows"]:
            # row: (article_id, source, url, title, body, language,
            #       published_at, scraped_at, bias_label, bias_confidence,
            #       bias_scores_json, embedding_str, event_id)
            assert len(row) == 13
            bias_label = row[8]
            bias_confidence = row[9]
            bias_scores_json = row[10]
            embedding_str = row[11]
            event_id = row[12]

            assert bias_label == "center"
            assert bias_confidence == pytest.approx(0.80)
            assert bias_scores_json is not None
            assert json.loads(bias_scores_json)["center"] == pytest.approx(0.80)
            assert embedding_str is not None and embedding_str.startswith("[")
            assert event_id == "evt-001"
