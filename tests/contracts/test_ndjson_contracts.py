"""
Contract tests for the NDJSON schemas between pipeline stages.

These tests verify that:
1. Valid data from each stage passes its schema
2. Invalid data is rejected with clear errors
3. Stage N output satisfies stage N+1 input schema
"""
from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from tests.contracts.schemas import (
    BiasOutput,
    CleanerOutput,
    ClusterOutput,
    EmbedderOutput,
    ScraperOutput,
)


# ── Fixtures: minimal valid records for each stage ──


@pytest.fixture
def scraper_record() -> dict:
    return {
        "article_id": "a1b2c3d4e5f6a1b2c3d4e5f6",
        "source": "hirunews",
        "url": "https://hirunews.lk/some-article",
        "title": "Test title",
        "body": "This is a test article body with enough characters to pass filtering.",
        "language": "si",
        "published_at": "2026-08-15T10:00:00",
        "scraped_at": "2026-08-15T12:00:00",
    }


@pytest.fixture
def cleaner_record(scraper_record) -> dict:
    return scraper_record  # same shape, normalized text


@pytest.fixture
def embedder_record() -> dict:
    return {
        "article_id": "a1b2c3d4e5f6a1b2c3d4e5f6",
        "embedding": [0.1] * 1024,
    }


@pytest.fixture
def bias_record() -> dict:
    return {
        "article_id": "a1b2c3d4e5f6a1b2c3d4e5f6",
        "bias_label": "center",
        "bias_confidence": 0.85,
        "bias_scores": {
            "far_left": 0.02,
            "left": 0.05,
            "center": 0.85,
            "right": 0.05,
            "far_right": 0.03,
        },
    }


@pytest.fixture
def cluster_record() -> dict:
    return {
        "article_id": "a1b2c3d4e5f6a1b2c3d4e5f6",
        "event_id": "550e8400-e29b-41d4-a716-446655440000",
        "cluster_method": "knn",
        "distance": 0.32,
    }


# ── Happy path: valid records parse correctly ──


class TestValidRecords:
    def test_scraper_output(self, scraper_record):
        record = ScraperOutput(**scraper_record)
        assert record.article_id == scraper_record["article_id"]
        assert record.source == "hirunews"

    def test_scraper_output_null_published_at(self, scraper_record):
        scraper_record["published_at"] = None
        record = ScraperOutput(**scraper_record)
        assert record.published_at is None

    def test_cleaner_output(self, cleaner_record):
        record = CleanerOutput(**cleaner_record)
        assert record.body == cleaner_record["body"]

    def test_embedder_output(self, embedder_record):
        record = EmbedderOutput(**embedder_record)
        assert len(record.embedding) == 1024

    def test_bias_output(self, bias_record):
        record = BiasOutput(**bias_record)
        assert record.bias_label == "center"
        assert record.bias_confidence == 0.85

    def test_cluster_output_knn(self, cluster_record):
        record = ClusterOutput(**cluster_record)
        assert record.cluster_method == "knn"

    def test_cluster_output_hdbscan(self, cluster_record):
        cluster_record["cluster_method"] = "hdbscan_new"
        record = ClusterOutput(**cluster_record)
        assert record.cluster_method == "hdbscan_new"

    def test_cluster_output_single(self, cluster_record):
        cluster_record["cluster_method"] = "single"
        record = ClusterOutput(**cluster_record)
        assert record.cluster_method == "single"


# ── Rejection: invalid data raises ValidationError ──


class TestInvalidRecords:
    def test_scraper_short_article_id(self, scraper_record):
        scraper_record["article_id"] = "tooshort"
        with pytest.raises(ValidationError, match="article_id"):
            ScraperOutput(**scraper_record)

    def test_scraper_empty_source(self, scraper_record):
        scraper_record["source"] = ""
        with pytest.raises(ValidationError, match="source"):
            ScraperOutput(**scraper_record)

    def test_scraper_missing_scraped_at(self, scraper_record):
        del scraper_record["scraped_at"]
        with pytest.raises(ValidationError, match="scraped_at"):
            ScraperOutput(**scraper_record)

    def test_cleaner_empty_body(self, cleaner_record):
        cleaner_record["body"] = ""
        with pytest.raises(ValidationError, match="body"):
            CleanerOutput(**cleaner_record)

    def test_embedder_empty_embedding(self, embedder_record):
        embedder_record["embedding"] = []
        with pytest.raises(ValidationError, match="embedding"):
            EmbedderOutput(**embedder_record)

    def test_bias_invalid_label(self, bias_record):
        bias_record["bias_label"] = "extreme_left"
        with pytest.raises(ValidationError, match="bias_label"):
            BiasOutput(**bias_record)

    def test_bias_confidence_out_of_range(self, bias_record):
        bias_record["bias_confidence"] = 1.5
        with pytest.raises(ValidationError, match="bias_confidence"):
            BiasOutput(**bias_record)

    def test_bias_missing_score_key(self, bias_record):
        del bias_record["bias_scores"]["far_left"]
        with pytest.raises(ValidationError, match="bias_scores"):
            BiasOutput(**bias_record)

    def test_cluster_invalid_method(self, cluster_record):
        cluster_record["cluster_method"] = "kmeans"
        with pytest.raises(ValidationError, match="cluster_method"):
            ClusterOutput(**cluster_record)

    def test_cluster_negative_distance(self, cluster_record):
        cluster_record["distance"] = -0.1
        with pytest.raises(ValidationError, match="distance"):
            ClusterOutput(**cluster_record)


# ── Cross-stage: stage N output satisfies stage N+1 input ──


class TestCrossStageCompatibility:
    """Verify that the output of one stage can be parsed as input to the next."""

    def test_scraper_to_cleaner(self, scraper_record):
        """Scraper output should be valid CleanerOutput input (same shape)."""
        scraper = ScraperOutput(**scraper_record)
        cleaner = CleanerOutput(**scraper.model_dump())
        assert cleaner.article_id == scraper.article_id

    def test_roundtrip_through_ndjson(self, scraper_record):
        """Serialize to JSON and back — simulates actual MinIO read/write."""
        original = ScraperOutput(**scraper_record)
        json_line = json.dumps(original.model_dump())
        parsed = json.loads(json_line)
        restored = CleanerOutput(**parsed)
        assert restored.article_id == original.article_id
        assert restored.body == original.body

    def test_all_stages_share_article_id_format(
        self, scraper_record, embedder_record, bias_record, cluster_record
    ):
        """All stages use the same article_id — the loader joins on it."""
        scraper = ScraperOutput(**scraper_record)
        embedder = EmbedderOutput(**embedder_record)
        bias = BiasOutput(**bias_record)
        cluster = ClusterOutput(**cluster_record)

        ids = {scraper.article_id, embedder.article_id, bias.article_id, cluster.article_id}
        assert len(ids) == 1, "All stages must share the same article_id format"

    def test_loader_can_join_all_stages(
        self, cleaner_record, embedder_record, bias_record, cluster_record
    ):
        """Simulate what the loader does: join all stage outputs by article_id."""
        clean = CleanerOutput(**cleaner_record)
        emb = EmbedderOutput(**embedder_record)
        bias = BiasOutput(**bias_record)
        cluster = ClusterOutput(**cluster_record)

        # Loader joins by article_id — all must match
        assert clean.article_id == emb.article_id == bias.article_id == cluster.article_id

        # Loader builds a complete row from all four
        row = {
            "article_id": clean.article_id,
            "source": clean.source,
            "url": clean.url,
            "title": clean.title,
            "body": clean.body,
            "language": clean.language,
            "published_at": clean.published_at,
            "scraped_at": clean.scraped_at,
            "embedding": emb.embedding,
            "bias_label": bias.bias_label,
            "bias_confidence": bias.bias_confidence,
            "bias_scores": bias.bias_scores,
            "event_id": cluster.event_id,
        }
        assert len(row) == 13
