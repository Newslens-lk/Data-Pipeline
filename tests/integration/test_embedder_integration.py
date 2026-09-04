"""
Integration test for the embedder container.

Full flow: seed cleaned articles in S3 -> mock model -> run main() ->
verify embeddings NDJSON in S3 with correct article_ids and dimensions.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from tests.conftest import load_container
from tests.contracts.schemas import EmbedderOutput
from tests.integration.conftest import BUCKET, read_ndjson, seed_ndjson


@pytest.fixture(scope="module")
def embedder():
    return load_container("embedder")


class TestEmbedderIntegration:
    def test_full_flow_embed_and_write(self, embedder, s3_mock, cleaned_articles):
        """Cleaned articles -> embedder main() -> embeddings NDJSON in S3."""
        input_key = "cleaned/2026-01-01/articles.ndjson"
        expected_out_key = "embeddings/2026-01-01/embeddings.ndjson"
        seed_ndjson(s3_mock, input_key, cleaned_articles)

        # Mock the embedding function to return fake 1024-dim vectors
        fake_embeddings = [[0.1] * 1024 for _ in range(len(cleaned_articles))]

        def mock_embed_local(texts, tokenizer, model, device):
            return [[0.1] * 1024 for _ in texts]

        mock_tokenizer = MagicMock()
        mock_model = MagicMock()

        with patch.object(embedder, "get_s3_client", return_value=s3_mock), \
             patch.object(embedder, "INPUT_KEY", input_key), \
             patch.object(embedder, "STORAGE_BUCKET", BUCKET), \
             patch.object(embedder, "USE_MODAL", False), \
             patch.object(embedder, "load_model", return_value=(mock_tokenizer, mock_model, "cpu")), \
             patch.object(embedder, "embed_batch_local", side_effect=mock_embed_local):
            embedder.main()

        output = read_ndjson(s3_mock, expected_out_key)

        # Same number of articles in, embeddings out
        assert len(output) == len(cleaned_articles)

        # Article IDs preserved
        input_ids = {a["article_id"] for a in cleaned_articles}
        output_ids = {r["article_id"] for r in output}
        assert input_ids == output_ids

        # Each embedding has correct dimension
        for record in output:
            assert len(record["embedding"]) == 1024
            EmbedderOutput(**record)

    def test_batching_produces_correct_count(self, embedder, s3_mock, cleaned_articles):
        """With BATCH_SIZE=2 and 3 articles, all 3 should still be embedded."""
        input_key = "cleaned/2026-01-01/articles.ndjson"
        seed_ndjson(s3_mock, input_key, cleaned_articles)

        def mock_embed_local(texts, tokenizer, model, device):
            return [[0.5] * 1024 for _ in texts]

        with patch.object(embedder, "get_s3_client", return_value=s3_mock), \
             patch.object(embedder, "INPUT_KEY", input_key), \
             patch.object(embedder, "STORAGE_BUCKET", BUCKET), \
             patch.object(embedder, "USE_MODAL", False), \
             patch.object(embedder, "BATCH_SIZE", 2), \
             patch.object(embedder, "load_model", return_value=(MagicMock(), MagicMock(), "cpu")), \
             patch.object(embedder, "embed_batch_local", side_effect=mock_embed_local):
            embedder.main()

        output = read_ndjson(s3_mock, "embeddings/2026-01-01/embeddings.ndjson")
        assert len(output) == 3

    def test_modal_path(self, embedder, s3_mock, cleaned_articles):
        """When USE_MODAL=True, embed_batch_modal is called instead of local."""
        input_key = "cleaned/2026-01-01/articles.ndjson"
        seed_ndjson(s3_mock, input_key, cleaned_articles)

        def mock_embed_modal(texts):
            return [[0.9] * 1024 for _ in texts]

        with patch.object(embedder, "get_s3_client", return_value=s3_mock), \
             patch.object(embedder, "INPUT_KEY", input_key), \
             patch.object(embedder, "STORAGE_BUCKET", BUCKET), \
             patch.object(embedder, "USE_MODAL", True), \
             patch.object(embedder, "embed_batch_modal", side_effect=mock_embed_modal):
            embedder.main()

        output = read_ndjson(s3_mock, "embeddings/2026-01-01/embeddings.ndjson")
        assert len(output) == 3
        assert output[0]["embedding"][0] == pytest.approx(0.9)
