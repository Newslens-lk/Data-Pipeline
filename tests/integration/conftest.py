"""
Shared fixtures for integration tests.

Uses moto to mock S3 (MinIO) so containers can read/write NDJSON
without a real object store. Each test gets a fresh bucket.
"""
from __future__ import annotations

import json
import os

import boto3
import pytest
from moto import mock_aws

from tests.conftest import load_container

BUCKET = "test-bucket"


@pytest.fixture
def s3_mock():
    """Mocked S3 with a pre-created bucket. Yields a boto3 client."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


def seed_ndjson(s3, key: str, records: list[dict]):
    """Helper: write a list of dicts as NDJSON to mock S3."""
    ndjson = "\n".join(json.dumps(r, ensure_ascii=False) for r in records)
    s3.put_object(Bucket=BUCKET, Key=key, Body=ndjson.encode("utf-8"))


def read_ndjson(s3, key: str) -> list[dict]:
    """Helper: read NDJSON from mock S3 and parse to list of dicts."""
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    lines = obj["Body"].read().decode("utf-8").strip().splitlines()
    return [json.loads(line) for line in lines]


@pytest.fixture
def cleaned_articles() -> list[dict]:
    """Three cleaned articles ready for embedder/bias input."""
    base = {
        "source": "hirunews",
        "language": "si",
        "published_at": "2026-08-15T10:00:00",
        "scraped_at": "2026-08-15T12:00:00",
    }
    return [
        {
            **base,
            "article_id": "aaa111bbb222ccc333ddd444",
            "url": "https://example.com/article-1",
            "title": "Government reforms economy",
            "body": "The government announced new economic reforms affecting the banking sector and trade policies.",
        },
        {
            **base,
            "source": "bbc_sinhala",
            "article_id": "bbb222ccc333ddd444eee555",
            "url": "https://example.com/article-2",
            "title": "Earthquake strikes coast",
            "body": "A major earthquake struck the coastal region early this morning causing widespread damage.",
        },
        {
            **base,
            "article_id": "ccc333ddd444eee555fff666",
            "url": "https://example.com/article-3",
            "title": "Cricket team wins series",
            "body": "The national cricket team won the test series against the visiting side in a decisive match.",
        },
    ]
