"""
Integration test for the cleaner container.

Full flow: seed raw NDJSON in S3 -> run main() -> verify cleaned output in S3.
"""
from __future__ import annotations

import json
from unittest.mock import patch

import pytest
from moto import mock_aws

from tests.conftest import load_container
from tests.contracts.schemas import CleanerOutput
from tests.integration.conftest import BUCKET, read_ndjson, seed_ndjson


@pytest.fixture(scope="module")
def cleaner():
    return load_container("cleaner")


class TestCleanerIntegration:
    def test_full_flow_clean_and_write(self, cleaner, s3_mock):
        """Raw articles -> cleaner main() -> cleaned NDJSON in S3."""
        raw_articles = [
            {
                "article_id": "aaa111bbb222ccc333ddd444",
                "source": "hirunews",
                "url": "https://example.com/1",
                "title": "  Hello  &amp;  World  ",
                "body": "A" * 100 + " &nbsp; some text &lt;tag&gt;",
                "language": "si",
                "published_at": "2026-08-15T10:00:00",
                "scraped_at": "2026-08-15T12:00:00",
            },
            {
                "article_id": "bbb222ccc333ddd444eee555",
                "source": "bbc_sinhala",
                "url": "https://example.com/2",
                "title": "Second Article",
                "body": "B" * 100,
                "language": "si",
                "published_at": None,
                "scraped_at": "2026-08-15T12:00:00",
            },
        ]
        input_key = "raw/2026-01-01/articles.ndjson"
        expected_out_key = "cleaned/2026-01-01/articles.ndjson"
        seed_ndjson(s3_mock, input_key, raw_articles)

        with patch.object(cleaner, "get_s3_client", return_value=s3_mock):
            with patch.object(cleaner, "INPUT_KEY", input_key):
                with patch.object(cleaner, "STORAGE_BUCKET", BUCKET):
                    cleaner.main()

        output = read_ndjson(s3_mock, expected_out_key)
        assert len(output) == 2

        # Verify text was cleaned
        assert output[0]["title"] == "Hello & World"
        assert "&nbsp;" not in output[0]["body"]
        assert "&lt;" not in output[0]["body"]

        # Validate all records against the contract schema
        for record in output:
            CleanerOutput(**record)

    def test_filters_short_articles(self, cleaner, s3_mock):
        """Articles with body < MIN_BODY_CHARS should be dropped."""
        raw_articles = [
            {
                "article_id": "aaa111bbb222ccc333ddd444",
                "source": "hirunews",
                "url": "https://example.com/1",
                "title": "Good article",
                "body": "X" * 100,
                "language": "si",
                "published_at": None,
                "scraped_at": "2026-08-15T12:00:00",
            },
            {
                "article_id": "bbb222ccc333ddd444eee555",
                "source": "hirunews",
                "url": "https://example.com/2",
                "title": "Too short",
                "body": "tiny",
                "language": "si",
                "published_at": None,
                "scraped_at": "2026-08-15T12:00:00",
            },
        ]
        input_key = "raw/2026-01-01/articles.ndjson"
        seed_ndjson(s3_mock, input_key, raw_articles)

        with patch.object(cleaner, "get_s3_client", return_value=s3_mock):
            with patch.object(cleaner, "INPUT_KEY", input_key):
                with patch.object(cleaner, "STORAGE_BUCKET", BUCKET):
                    cleaner.main()

        output = read_ndjson(s3_mock, "cleaned/2026-01-01/articles.ndjson")
        assert len(output) == 1
        assert output[0]["article_id"] == "aaa111bbb222ccc333ddd444"

    def test_deduplicates_near_identical(self, cleaner, s3_mock):
        """Two articles with the same first 300 chars -> only first kept."""
        shared_body = "Z" * 300
        raw_articles = [
            {
                "article_id": "aaa111bbb222ccc333ddd444",
                "source": "hirunews",
                "url": "https://example.com/1",
                "title": "Title A",
                "body": shared_body + " unique ending one",
                "language": "si",
                "published_at": None,
                "scraped_at": "2026-08-15T12:00:00",
            },
            {
                "article_id": "bbb222ccc333ddd444eee555",
                "source": "lankadeepa",
                "url": "https://example.com/2",
                "title": "Title B",
                "body": shared_body + " unique ending two",
                "language": "si",
                "published_at": None,
                "scraped_at": "2026-08-15T12:00:00",
            },
        ]
        input_key = "raw/2026-01-01/articles.ndjson"
        seed_ndjson(s3_mock, input_key, raw_articles)

        with patch.object(cleaner, "get_s3_client", return_value=s3_mock):
            with patch.object(cleaner, "INPUT_KEY", input_key):
                with patch.object(cleaner, "STORAGE_BUCKET", BUCKET):
                    cleaner.main()

        output = read_ndjson(s3_mock, "cleaned/2026-01-01/articles.ndjson")
        assert len(output) == 1
