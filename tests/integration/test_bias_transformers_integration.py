"""
Integration test for the HelaBERT bias classifier container.

Full flow: seed cleaned articles in S3 -> mock HelaBERT model -> run main() ->
verify bias results NDJSON in S3. Tests both local and Modal paths.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from tests.conftest import load_container
from tests.contracts.schemas import BiasOutput
from tests.integration.conftest import BUCKET, read_ndjson, seed_ndjson


@pytest.fixture(scope="module")
def bias_tf():
    return load_container("bias-classifier-transformers")


class TestBiasTransformersIntegration:
    def test_full_flow_local_classify(self, bias_tf, s3_mock, cleaned_articles):
        """Cleaned articles -> bias-transformers main() (local) -> bias results in S3."""
        input_key = "cleaned/2026-01-01/articles.ndjson"
        expected_out_key = "bias/2026-01-01/bias_results.ndjson"
        seed_ndjson(s3_mock, input_key, cleaned_articles)

        # Mock classify_batch_local: return fixed probabilities per batch
        def mock_classify_local(texts, sp, model, device):
            return [[0.05, 0.10, 0.70, 0.10, 0.05]] * len(texts)

        with patch.object(bias_tf, "get_s3_client", return_value=s3_mock), \
             patch.object(bias_tf, "INPUT_KEY", input_key), \
             patch.object(bias_tf, "STORAGE_BUCKET", BUCKET), \
             patch.object(bias_tf, "USE_MODAL", False), \
             patch.object(bias_tf, "load_model", return_value=(MagicMock(), MagicMock(), "cpu")), \
             patch.object(bias_tf, "classify_batch_local", side_effect=mock_classify_local):
            bias_tf.main()

        output = read_ndjson(s3_mock, expected_out_key)
        assert len(output) == len(cleaned_articles)

        # All should be "center" with the mock probs
        for record in output:
            BiasOutput(**record)
            assert record["bias_label"] == "center"
            assert record["bias_confidence"] == pytest.approx(0.70)

        # Article IDs match input
        input_ids = {a["article_id"] for a in cleaned_articles}
        output_ids = {r["article_id"] for r in output}
        assert input_ids == output_ids

    def test_modal_path_classify(self, bias_tf, s3_mock, cleaned_articles):
        """When USE_MODAL=True, classify_batch_modal is called."""
        input_key = "cleaned/2026-01-01/articles.ndjson"
        seed_ndjson(s3_mock, input_key, cleaned_articles)

        def mock_classify_modal(texts):
            return [[0.80, 0.05, 0.05, 0.05, 0.05]] * len(texts)

        with patch.object(bias_tf, "get_s3_client", return_value=s3_mock), \
             patch.object(bias_tf, "INPUT_KEY", input_key), \
             patch.object(bias_tf, "STORAGE_BUCKET", BUCKET), \
             patch.object(bias_tf, "USE_MODAL", True), \
             patch.object(bias_tf, "classify_batch_modal", side_effect=mock_classify_modal):
            bias_tf.main()

        output = read_ndjson(s3_mock, "bias/2026-01-01/bias_results.ndjson")
        assert len(output) == 3
        for record in output:
            assert record["bias_label"] == "far_left"

    def test_output_schema_matches_xgb(self, bias_tf, s3_mock, cleaned_articles):
        """Transformers and XGB containers must produce identical output schemas."""
        input_key = "cleaned/2026-01-01/articles.ndjson"
        seed_ndjson(s3_mock, input_key, cleaned_articles[:1])

        def mock_classify_local(texts, sp, model, device):
            return [[0.05, 0.05, 0.80, 0.05, 0.05]] * len(texts)

        with patch.object(bias_tf, "get_s3_client", return_value=s3_mock), \
             patch.object(bias_tf, "INPUT_KEY", input_key), \
             patch.object(bias_tf, "STORAGE_BUCKET", BUCKET), \
             patch.object(bias_tf, "USE_MODAL", False), \
             patch.object(bias_tf, "load_model", return_value=(MagicMock(), MagicMock(), "cpu")), \
             patch.object(bias_tf, "classify_batch_local", side_effect=mock_classify_local):
            bias_tf.main()

        output = read_ndjson(s3_mock, "bias/2026-01-01/bias_results.ndjson")
        record = output[0]
        # Must have exactly these keys — same as XGB output
        assert set(record.keys()) == {"article_id", "bias_label", "bias_confidence", "bias_scores"}
        assert set(record["bias_scores"].keys()) == {"far_left", "left", "center", "right", "far_right"}
