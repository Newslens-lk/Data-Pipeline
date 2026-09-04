"""Unit tests for the Config dataclass."""
from __future__ import annotations

import pytest

from include.config import CONFIG, Config


class TestConfig:
    def test_singleton_instance_exists(self):
        assert CONFIG is not None
        assert isinstance(CONFIG, Config)

    def test_frozen_immutability(self):
        with pytest.raises(AttributeError):
            CONFIG.embedding_dim = 512

    def test_bias_labels_defined(self):
        assert len(CONFIG.bias_labels) >= 2

    def test_hdbscan_min_cluster_size_positive(self):
        assert CONFIG.hdbscan_min_cluster_size >= 2

    def test_hdbscan_metric_is_euclidean(self):
        assert CONFIG.hdbscan_metric == "euclidean"

    def test_embedding_dim_positive(self):
        assert CONFIG.embedding_dim > 0

    def test_embedding_batch_size_positive(self):
        assert CONFIG.embedding_batch_size > 0

    def test_bias_batch_size_positive(self):
        assert CONFIG.bias_batch_size > 0

    def test_llm_max_tokens_positive(self):
        assert CONFIG.llm_max_tokens > 0
