"""
End-to-end tests for the full NewsLens pipeline.

Runs every container's main() in sequence with a single shared moto S3 instance,
verifying that each stage's output feeds correctly into the next. Tests both
pipeline paths:

    Path A (XGB):          scraper -> cleaner -> embedder -> bias-xgb -> clustering -> loader
    Path B (Transformers): scraper -> cleaner -> embedder -> bias-transformers -> clustering -> loader

ML models and DB are mocked, but the full S3 read/write chain and NDJSON
serialization between stages is real.
"""
from __future__ import annotations

import json
import uuid
from unittest.mock import MagicMock, patch

import boto3
import numpy as np
import pytest
from moto import mock_aws

from tests.conftest import load_container
from tests.contracts.schemas import (
    BiasOutput,
    CleanerOutput,
    ClusterOutput,
    EmbedderOutput,
    ScraperOutput,
)

BUCKET = "test-bucket"
DATE = "2026-01-01"

# ── S3 Keys (mirrors the real pipeline key derivation) ──

RAW_KEY = f"raw/{DATE}/articles.ndjson"
CLEAN_KEY = f"cleaned/{DATE}/articles.ndjson"
EMBEDDINGS_KEY = f"embeddings/{DATE}/embeddings.ndjson"
BIAS_KEY = f"bias/{DATE}/bias_results.ndjson"
CLUSTERS_KEY = f"clusters/{DATE}/cluster_assignments.ndjson"


# ── Test Data ──

RAW_ARTICLES = [
    {
        "article_id": "aaa111bbb222ccc333ddd444",
        "source": "hirunews",
        "url": "https://hirunews.lk/test-1",
        "title": "  Government  &amp;  Economy  ",
        "body": "The government announced new economic reforms affecting the banking sector. "
                "These reforms will change how banks operate across the country and impact lending rates. " * 3,
        "language": "si",
        "published_at": "2026-08-15T10:00:00",
        "scraped_at": "2026-08-15T12:00:00",
    },
    {
        "article_id": "bbb222ccc333ddd444eee555",
        "source": "bbc_sinhala",
        "url": "https://bbc.com/sinhala/test-2",
        "title": "Earthquake  Strikes  Coast",
        "body": "A major earthquake struck the coastal region early this morning causing widespread damage. "
                "Rescue teams have been deployed to the affected areas and emergency shelters opened. " * 3,
        "language": "si",
        "published_at": "2026-08-15T11:00:00",
        "scraped_at": "2026-08-15T12:00:00",
    },
    {
        "article_id": "ccc333ddd444eee555fff666",
        "source": "lankadeepa",
        "url": "https://lankadeepa.lk/test-3",
        "title": "Cricket Team  Wins  Series",
        "body": "The national cricket team won the test series against the visiting side in a decisive match. "
                "Fans celebrated across the country as the team secured a historic victory. " * 3,
        "language": "si",
        "published_at": "2026-08-15T09:00:00",
        "scraped_at": "2026-08-15T12:00:00",
    },
    {
        "article_id": "ddd444eee555fff666aaa111",
        "source": "hirunews",
        "url": "https://hirunews.lk/test-4",
        "title": "Too Short",
        "body": "tiny",  # should be filtered by cleaner
        "language": "si",
        "published_at": None,
        "scraped_at": "2026-08-15T12:00:00",
    },
]

ARTICLE_IDS = [a["article_id"] for a in RAW_ARTICLES[:3]]  # 4th gets filtered


# ── Fixtures ──


@pytest.fixture(scope="module")
def containers():
    """Load all container modules once."""
    return {
        "cleaner": load_container("cleaner"),
        "embedder": load_container("embedder"),
        "bias_xgb": load_container("bias-classifier-xgb"),
        "bias_tf": load_container("bias-classifier-transformers"),
        "clustering": load_container("clustering"),
        "loader": load_container("loader"),
    }


def _seed_raw(s3):
    """Seed S3 with raw scraped articles."""
    ndjson = "\n".join(json.dumps(a, ensure_ascii=False) for a in RAW_ARTICLES)
    s3.put_object(Bucket=BUCKET, Key=RAW_KEY, Body=ndjson.encode("utf-8"))


def _read_ndjson(s3, key):
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return [json.loads(line) for line in obj["Body"].read().decode("utf-8").strip().splitlines()]


def _run_cleaner(s3, cleaner):
    with patch.object(cleaner, "get_s3_client", return_value=s3), \
         patch.object(cleaner, "INPUT_KEY", RAW_KEY), \
         patch.object(cleaner, "STORAGE_BUCKET", BUCKET):
        cleaner.main()


def _run_embedder(s3, embedder):
    def mock_embed(texts, tokenizer, model, device):
        return [[0.1 * (i + 1)] * 1024 for i, _ in enumerate(texts)]

    with patch.object(embedder, "get_s3_client", return_value=s3), \
         patch.object(embedder, "INPUT_KEY", CLEAN_KEY), \
         patch.object(embedder, "STORAGE_BUCKET", BUCKET), \
         patch.object(embedder, "USE_MODAL", False), \
         patch.object(embedder, "load_model", return_value=(MagicMock(), MagicMock(), "cpu")), \
         patch.object(embedder, "embed_batch_local", side_effect=mock_embed):
        embedder.main()


def _run_bias_xgb(s3, bias_xgb, n_articles):
    mock_model = MagicMock()
    mock_model.predict_proba.return_value = np.array([
        [0.05, 0.10, 0.70, 0.10, 0.05],  # center
        [0.60, 0.20, 0.10, 0.05, 0.05],  # far_left
        [0.05, 0.05, 0.10, 0.20, 0.60],  # far_right
    ][:n_articles])

    with patch.object(bias_xgb, "get_s3_client", return_value=s3), \
         patch.object(bias_xgb, "INPUT_KEY", EMBEDDINGS_KEY), \
         patch.object(bias_xgb, "STORAGE_BUCKET", BUCKET), \
         patch.object(bias_xgb, "load_model", return_value=mock_model):
        bias_xgb.main()


def _run_bias_transformers(s3, bias_tf):
    def mock_classify(texts, sp, model, device):
        probs_cycle = [
            [0.05, 0.10, 0.70, 0.10, 0.05],  # center
            [0.60, 0.20, 0.10, 0.05, 0.05],  # far_left
            [0.05, 0.05, 0.10, 0.20, 0.60],  # far_right
        ]
        return [probs_cycle[i % 3] for i in range(len(texts))]

    with patch.object(bias_tf, "get_s3_client", return_value=s3), \
         patch.object(bias_tf, "INPUT_KEY", CLEAN_KEY), \
         patch.object(bias_tf, "STORAGE_BUCKET", BUCKET), \
         patch.object(bias_tf, "USE_MODAL", False), \
         patch.object(bias_tf, "load_model", return_value=(MagicMock(), MagicMock(), "cpu")), \
         patch.object(bias_tf, "classify_batch_local", side_effect=mock_classify):
        bias_tf.main()


def _run_clustering(s3, clustering):
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_cur.fetchall.return_value = []  # no existing clusters
    mock_conn.cursor.return_value = mock_cur

    with patch.object(clustering, "get_s3_client", return_value=s3), \
         patch.object(clustering, "get_db_connection", return_value=mock_conn), \
         patch.object(clustering, "INPUT_KEY", EMBEDDINGS_KEY), \
         patch.object(clustering, "STORAGE_BUCKET", BUCKET):
        clustering.main()


def _run_loader(s3, loader):
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value = mock_cur

    captured = []

    def capture_ev(cur, sql, rows, *args, **kwargs):
        captured.append({"sql": sql, "rows": list(rows)})

    with patch.object(loader, "get_s3_client", return_value=s3), \
         patch.object(loader, "CLEAN_KEY", CLEAN_KEY), \
         patch.object(loader, "EMBEDDINGS_KEY", EMBEDDINGS_KEY), \
         patch.object(loader, "BIAS_KEY", BIAS_KEY), \
         patch.object(loader, "CLUSTERS_KEY", CLUSTERS_KEY), \
         patch.object(loader, "STORAGE_BUCKET", BUCKET), \
         patch.object(loader, "execute_values", capture_ev), \
         patch("psycopg2.connect", return_value=mock_conn):
        loader.main()

    return captured, mock_conn


# ── Path A: XGBoost Pipeline ──


class TestFullPipelineXGB:
    """End-to-end: scraper output -> cleaner -> embedder -> bias-xgb -> clustering -> loader."""

    @pytest.fixture(autouse=True)
    def run_pipeline(self, containers):
        """Run the full XGB pipeline once, store results for all tests."""
        with mock_aws():
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket=BUCKET)
            _seed_raw(s3)

            # Stage 1: Clean
            _run_cleaner(s3, containers["cleaner"])
            self.cleaned = _read_ndjson(s3, CLEAN_KEY)

            # Stage 2: Embed
            _run_embedder(s3, containers["embedder"])
            self.embeddings = _read_ndjson(s3, EMBEDDINGS_KEY)

            # Stage 3: Bias (XGB path — reads embeddings)
            _run_bias_xgb(s3, containers["bias_xgb"], len(self.cleaned))
            self.bias_results = _read_ndjson(s3, BIAS_KEY)

            # Stage 4: Cluster
            _run_clustering(s3, containers["clustering"])
            self.cluster_assignments = _read_ndjson(s3, CLUSTERS_KEY)

            # Stage 5: Load
            self.loader_calls, self.mock_conn = _run_loader(s3, containers["loader"])

            yield

    # ── Cleaner stage ──

    def test_cleaner_filters_short_article(self):
        assert len(self.cleaned) == 3  # 4th article too short

    def test_cleaner_normalizes_text(self):
        titles = {a["title"] for a in self.cleaned}
        assert "Government & Economy" in titles  # &amp; -> &, whitespace collapsed

    def test_cleaner_output_valid(self):
        for record in self.cleaned:
            CleanerOutput(**record)

    # ── Embedder stage ──

    def test_embedder_matches_cleaned_count(self):
        assert len(self.embeddings) == len(self.cleaned)

    def test_embedder_preserves_article_ids(self):
        clean_ids = {a["article_id"] for a in self.cleaned}
        emb_ids = {r["article_id"] for r in self.embeddings}
        assert clean_ids == emb_ids

    def test_embedder_output_valid(self):
        for record in self.embeddings:
            emb = EmbedderOutput(**record)
            assert len(emb.embedding) == 1024

    # ── Bias-XGB stage ──

    def test_bias_xgb_matches_count(self):
        assert len(self.bias_results) == len(self.cleaned)

    def test_bias_xgb_output_valid(self):
        for record in self.bias_results:
            BiasOutput(**record)

    def test_bias_xgb_labels_assigned(self):
        labels = {r["bias_label"] for r in self.bias_results}
        assert labels.issubset({"far_left", "left", "center", "right", "far_right"})

    # ── Clustering stage ──

    def test_clustering_matches_count(self):
        assert len(self.cluster_assignments) == len(self.cleaned)

    def test_clustering_output_valid(self):
        for record in self.cluster_assignments:
            ClusterOutput(**record)

    def test_clustering_all_have_event_ids(self):
        for record in self.cluster_assignments:
            assert record["event_id"] is not None
            uuid.UUID(record["event_id"])

    # ── Loader stage ──

    def test_loader_makes_three_db_calls(self):
        assert len(self.loader_calls) == 3
        assert "sources" in self.loader_calls[0]["sql"]
        assert "events" in self.loader_calls[1]["sql"]
        assert "articles" in self.loader_calls[2]["sql"]

    def test_loader_sources_correct(self):
        source_names = {row[0] for row in self.loader_calls[0]["rows"]}
        assert source_names == {"hirunews", "bbc_sinhala", "lankadeepa"}

    def test_loader_articles_have_all_fields(self):
        for row in self.loader_calls[2]["rows"]:
            assert len(row) == 13
            bias_label = row[8]
            embedding_str = row[11]
            event_id = row[12]
            assert bias_label in ("far_left", "left", "center", "right", "far_right")
            assert embedding_str is not None and embedding_str.startswith("[")
            assert event_id is not None

    def test_loader_commits(self):
        self.mock_conn.commit.assert_called_once()

    # ── Full chain validation ──

    def test_article_ids_consistent_across_all_stages(self):
        clean_ids = {a["article_id"] for a in self.cleaned}
        emb_ids = {r["article_id"] for r in self.embeddings}
        bias_ids = {r["article_id"] for r in self.bias_results}
        cluster_ids = {r["article_id"] for r in self.cluster_assignments}
        loader_ids = {row[0] for row in self.loader_calls[2]["rows"]}

        assert clean_ids == emb_ids == bias_ids == cluster_ids == loader_ids


# ── Path B: Transformers Pipeline ──


class TestFullPipelineTransformers:
    """End-to-end: scraper output -> cleaner -> embedder -> bias-transformers -> clustering -> loader."""

    @pytest.fixture(autouse=True)
    def run_pipeline(self, containers):
        """Run the full Transformers pipeline once, store results for all tests."""
        with mock_aws():
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket=BUCKET)
            _seed_raw(s3)

            # Stage 1: Clean
            _run_cleaner(s3, containers["cleaner"])
            self.cleaned = _read_ndjson(s3, CLEAN_KEY)

            # Stage 2: Embed (still needed for clustering + loader)
            _run_embedder(s3, containers["embedder"])
            self.embeddings = _read_ndjson(s3, EMBEDDINGS_KEY)

            # Stage 3: Bias (Transformers path — reads cleaned text, not embeddings)
            _run_bias_transformers(s3, containers["bias_tf"])
            self.bias_results = _read_ndjson(s3, BIAS_KEY)

            # Stage 4: Cluster
            _run_clustering(s3, containers["clustering"])
            self.cluster_assignments = _read_ndjson(s3, CLUSTERS_KEY)

            # Stage 5: Load
            self.loader_calls, self.mock_conn = _run_loader(s3, containers["loader"])

            yield

    # ── Bias-Transformers stage ──

    def test_bias_tf_matches_count(self):
        assert len(self.bias_results) == len(self.cleaned)

    def test_bias_tf_output_valid(self):
        for record in self.bias_results:
            BiasOutput(**record)

    def test_bias_tf_reads_cleaned_not_embeddings(self):
        """Transformers classifier should have article_ids from cleaned articles, not embeddings."""
        clean_ids = {a["article_id"] for a in self.cleaned}
        bias_ids = {r["article_id"] for r in self.bias_results}
        assert clean_ids == bias_ids

    def test_bias_tf_labels_assigned(self):
        labels = {r["bias_label"] for r in self.bias_results}
        assert labels.issubset({"far_left", "left", "center", "right", "far_right"})

    # ── Loader stage (same schema regardless of bias classifier used) ──

    def test_loader_articles_have_bias_from_transformers(self):
        for row in self.loader_calls[2]["rows"]:
            assert len(row) == 13
            bias_label = row[8]
            assert bias_label in ("far_left", "left", "center", "right", "far_right")

    def test_loader_commits(self):
        self.mock_conn.commit.assert_called_once()

    # ── Full chain validation ──

    def test_article_ids_consistent_across_all_stages(self):
        clean_ids = {a["article_id"] for a in self.cleaned}
        emb_ids = {r["article_id"] for r in self.embeddings}
        bias_ids = {r["article_id"] for r in self.bias_results}
        cluster_ids = {r["article_id"] for r in self.cluster_assignments}
        loader_ids = {row[0] for row in self.loader_calls[2]["rows"]}

        assert clean_ids == emb_ids == bias_ids == cluster_ids == loader_ids

    def test_both_paths_produce_same_loader_schema(self):
        """Loader output shape is identical regardless of which bias classifier was used."""
        for row in self.loader_calls[2]["rows"]:
            # 13 fields: article_id, source, url, title, body, language,
            #            published_at, scraped_at, bias_label, bias_confidence,
            #            bias_scores_json, embedding_str, event_id
            assert len(row) == 13
