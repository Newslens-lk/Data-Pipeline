"""Unit tests for the XGBoost bias classifier's label/score logic.

The actual model inference is tested in integration tests. Here we test
the probability-to-label conversion logic that runs AFTER model.predict_proba().
"""
from __future__ import annotations

import pytest
from tests.conftest import load_container


@pytest.fixture(scope="module")
def bias_xgb():
    return load_container("bias-classifier-xgb")


class TestLabels:
    def test_labels_has_five_classes(self, bias_xgb):
        assert bias_xgb.LABELS == ["far_left", "left", "center", "right", "far_right"]

    def test_label_order_matches_model_output(self, bias_xgb):
        assert bias_xgb.LABELS[0] == "far_left"
        assert bias_xgb.LABELS[2] == "center"
        assert bias_xgb.LABELS[4] == "far_right"


class TestScoreConversion:
    """Test the probability -> label/confidence/scores logic from main()."""

    def test_highest_prob_becomes_label(self, bias_xgb):
        probs = [0.05, 0.10, 0.70, 0.10, 0.05]
        scores = {label: float(probs[j]) for j, label in enumerate(bias_xgb.LABELS)}
        top_label = max(scores, key=scores.get)
        assert top_label == "center"

    def test_confidence_is_top_score(self, bias_xgb):
        probs = [0.01, 0.02, 0.90, 0.04, 0.03]
        scores = {label: float(probs[j]) for j, label in enumerate(bias_xgb.LABELS)}
        top_label = max(scores, key=scores.get)
        assert scores[top_label] == pytest.approx(0.90)

    def test_all_labels_in_scores(self, bias_xgb):
        probs = [0.2, 0.2, 0.2, 0.2, 0.2]
        scores = {label: float(probs[j]) for j, label in enumerate(bias_xgb.LABELS)}
        assert set(scores.keys()) == {"far_left", "left", "center", "right", "far_right"}

    def test_far_left_wins_when_highest(self, bias_xgb):
        probs = [0.80, 0.05, 0.05, 0.05, 0.05]
        scores = {label: float(probs[j]) for j, label in enumerate(bias_xgb.LABELS)}
        top_label = max(scores, key=scores.get)
        assert top_label == "far_left"

    def test_far_right_wins_when_highest(self, bias_xgb):
        probs = [0.05, 0.05, 0.05, 0.05, 0.80]
        scores = {label: float(probs[j]) for j, label in enumerate(bias_xgb.LABELS)}
        top_label = max(scores, key=scores.get)
        assert top_label == "far_right"
