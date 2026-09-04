"""
Shared fixtures for the NewsLens pipeline test suite.

Container run.py files read environment variables at module level, so we
must set dummy env vars BEFORE importing them. The `container_env` fixture
(autouse, session-scoped) handles this.
"""
from __future__ import annotations

import os
import sys

import pytest


@pytest.fixture(autouse=True, scope="session")
def container_env():
    """Set dummy env vars so container run.py files can be imported safely."""
    env_defaults = {
        "INPUT_KEY": "test/2026-01-01/articles.ndjson",
        "CLEAN_KEY": "cleaned/2026-01-01/articles.ndjson",
        "EMBEDDINGS_KEY": "embeddings/2026-01-01/embeddings.ndjson",
        "BIAS_KEY": "bias/2026-01-01/bias_results.ndjson",
        "CLUSTERS_KEY": "clusters/2026-01-01/cluster_assignments.ndjson",
        "STORAGE_ENDPOINT": "http://localhost:9000",
        "STORAGE_BUCKET": "test-bucket",
        "AWS_ACCESS_KEY_ID": "test",
        "AWS_SECRET_ACCESS_KEY": "test",
        "MODEL_KEY": "models/test.joblib",
        "DB_HOST": "localhost",
        "DB_PORT": "5432",
        "DB_NAME": "test_db",
        "DB_USER": "test",
        "DB_PASSWORD": "test",
    }
    originals = {}
    for key, val in env_defaults.items():
        originals[key] = os.environ.get(key)
        os.environ.setdefault(key, val)

    yield

    for key, orig in originals.items():
        if orig is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = orig


def load_container(name: str):
    """Load a container's run.py as a uniquely-named module.

    Each container has its own run.py, so we use spec_from_file_location
    with a unique module name to avoid collisions.
    """
    import importlib.util

    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(base, "containers", name, "run.py")
    module_name = f"container_{name.replace('-', '_')}_run"

    # Return cached module if already loaded
    if module_name in sys.modules:
        return sys.modules[module_name]

    spec = importlib.util.spec_from_file_location(module_name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


# ── Sample data fixtures ──

@pytest.fixture
def sample_article() -> dict:
    """A minimal valid cleaned article dict."""
    return {
        "article_id": "a1b2c3d4e5f6a1b2c3d4e5f6",
        "source": "hirunews",
        "url": "https://hirunews.lk/test-article",
        "title": "Test headline",
        "body": "This is a sufficiently long article body for testing purposes in the pipeline.",
        "language": "si",
        "published_at": "2026-08-15T10:00:00",
        "scraped_at": "2026-08-15T12:00:00",
    }


@pytest.fixture
def sample_articles() -> list[dict]:
    """Three cleaned articles with distinct content."""
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
            "title": "First article title",
            "body": "The government announced new economic reforms affecting the banking sector.",
        },
        {
            **base,
            "article_id": "bbb222ccc333ddd444eee555",
            "url": "https://example.com/article-2",
            "title": "Second article title",
            "body": "A major earthquake struck the coastal region early this morning.",
        },
        {
            **base,
            "article_id": "ccc333ddd444eee555fff666",
            "url": "https://example.com/article-3",
            "title": "Third article title",
            "body": "The national cricket team won the test series against the visiting side.",
        },
    ]
