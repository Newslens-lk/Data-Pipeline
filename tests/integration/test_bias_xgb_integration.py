"""
Integration test for the XGBoost bias classifier container.

Full flow: seed embeddings in S3 -> mock XGBoost model -> run main() ->
verify bias results NDJSON in S3 with correct labels and scores.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from tests.conftest import load_container
from tests.contracts.schemas import BiasOutput
from tests.integration.conftest import BUCKET, read_ndjson, seed_ndjson


@pytest.fixture(scope="module")
def bias_xgb():
    return load_container("bias-classifier-xgb")


def _make_embeddings(article_ids: list[str], dim: int = 1024) -> list[dict]:
    return [
        {"article_id": aid, "embedding": [0.1] * dim}
        for aid in article_ids
    ]


class TestBiasXgbIntegration:
    def test_full_flow_classify_and_write(self, bias_xgb, s3_mock):
        """Embeddings -> bias-xgb main() -> bias results in S3."""
        article_ids = [
            "aaa111bbb222ccc333ddd444",
            "bbb222ccc333ddd444eee555",
            "ccc333ddd444eee555fff666",
        ]
        input_key = "embeddings/2026-01-01/embeddings.ndjson"
        expected_out_key = "bias/2026-01-01/bias_results.ndjson"
        seed_ndjson(s3_mock, input_key, _make_embeddings(article_ids))

        # Mock XGBoost model: returns fixed probabilities
        mock_model = MagicMock()
        mock_model.predict_proba.return_value = np.array([
            [0.05, 0.10, 0.70, 0.10, 0.05],  # center
            [0.60, 0.20, 0.10, 0.05, 0.05],  # far_left
            [0.05, 0.05, 0.10, 0.20, 0.60],  # far_right
        ])

        with patch.object(bias_xgb, "get_s3_client", return_value=s3_mock), \
             patch.object(bias_xgb, "INPUT_KEY", input_key), \
             patch.object(bias_xgb, "STORAGE_BUCKET", BUCKET), \
             patch.object(bias_xgb, "load_model", return_value=mock_model):
            bias_xgb.main()

        output = read_ndjson(s3_mock, expected_out_key)

        assert len(output) == 3

        # Validate each record against schema
        for record in output:
            BiasOutput(**record)

        # Check expected labels
        label_map = {r["article_id"]: r["bias_label"] for r in output}
        assert label_map["aaa111bbb222ccc333ddd444"] == "center"
        assert label_map["bbb222ccc333ddd444eee555"] == "far_left"
        assert label_map["ccc333ddd444eee555fff666"] == "far_right"

    def test_confidence_matches_top_score(self, bias_xgb, s3_mock):
        """bias_confidence should equal the score of the predicted label."""
        article_ids = ["aaa111bbb222ccc333ddd444"]
        input_key = "embeddings/2026-01-01/embeddings.ndjson"
        seed_ndjson(s3_mock, input_key, _make_embeddings(article_ids))

        mock_model = MagicMock()
        mock_model.predict_proba.return_value = np.array([
            [0.02, 0.03, 0.85, 0.07, 0.03],
        ])

        with patch.object(bias_xgb, "get_s3_client", return_value=s3_mock), \
             patch.object(bias_xgb, "INPUT_KEY", input_key), \
             patch.object(bias_xgb, "STORAGE_BUCKET", BUCKET), \
             patch.object(bias_xgb, "load_model", return_value=mock_model):
            bias_xgb.main()

        output = read_ndjson(s3_mock, "bias/2026-01-01/bias_results.ndjson")
        record = output[0]
        assert record["bias_confidence"] == pytest.approx(0.85)
        assert record["bias_scores"]["center"] == pytest.approx(0.85)

    def test_all_five_scores_present(self, bias_xgb, s3_mock):
        """Every output record must have all 5 bias_scores keys."""
        article_ids = ["aaa111bbb222ccc333ddd444"]
        input_key = "embeddings/2026-01-01/embeddings.ndjson"
        seed_ndjson(s3_mock, input_key, _make_embeddings(article_ids))

        mock_model = MagicMock()
        mock_model.predict_proba.return_value = np.array([[0.2, 0.2, 0.2, 0.2, 0.2]])

        with patch.object(bias_xgb, "get_s3_client", return_value=s3_mock), \
             patch.object(bias_xgb, "INPUT_KEY", input_key), \
             patch.object(bias_xgb, "STORAGE_BUCKET", BUCKET), \
             patch.object(bias_xgb, "load_model", return_value=mock_model):
            bias_xgb.main()

        output = read_ndjson(s3_mock, "bias/2026-01-01/bias_results.ndjson")
        assert set(output[0]["bias_scores"].keys()) == {"far_left", "left", "center", "right", "far_right"}
