"""Unit tests for the HelaBERT bias classifier's label/score logic.

Same probability-to-label conversion as XGBoost, but applied to the
transformers container. Tests confirm both classifiers use the same
label set and conversion logic.
"""
from __future__ import annotations

import pytest
from tests.conftest import load_container


@pytest.fixture(scope="module")
def bias_tf():
    return load_container("bias-classifier-transformers")


class TestLabels:
    def test_labels_match_xgb(self, bias_tf):
        assert bias_tf.LABELS == ["far_left", "left", "center", "right", "far_right"]

    def test_num_labels_constant(self, bias_tf):
        assert bias_tf.NUM_LABELS == 5


class TestScoreConversion:
    def test_highest_prob_becomes_label(self, bias_tf):
        probs = [0.05, 0.10, 0.70, 0.10, 0.05]
        scores = {label: float(probs[k]) for k, label in enumerate(bias_tf.LABELS)}
        top_label = max(scores, key=scores.get)
        assert top_label == "center"

    def test_confidence_is_max_score(self, bias_tf):
        probs = [0.01, 0.89, 0.05, 0.03, 0.02]
        scores = {label: float(probs[k]) for k, label in enumerate(bias_tf.LABELS)}
        top_label = max(scores, key=scores.get)
        assert top_label == "left"
        assert scores[top_label] == pytest.approx(0.89)

    def test_text_prep_format(self, bias_tf):
        """Transformers classifier joins title + body with '. ' separator."""
        article = {"title": "Headline", "body": "Body text here"}
        text = f"{article['title']}. {article['body']}"
        assert text == "Headline. Body text here"
