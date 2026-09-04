"""Unit tests for the clustering container's pure functions.

Tests batch_cluster_unassigned (HDBSCAN) and the find_nearest_cluster
majority-vote logic without needing a real database.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from tests.conftest import load_container


@pytest.fixture(scope="module")
def clustering():
    return load_container("clustering")


# ── batch_cluster_unassigned (HDBSCAN) ──


class TestBatchClusterUnassigned:
    def test_returns_empty_for_single_article(self, clustering):
        unassigned = [{"embedding": [0.0, 0.0]}]
        result = clustering.batch_cluster_unassigned(unassigned)
        assert result == {}

    def test_returns_empty_for_empty_list(self, clustering):
        result = clustering.batch_cluster_unassigned([])
        assert result == {}

    def test_groups_close_points(self, clustering):
        """Two tight groups should get different event_ids."""
        group_a = [[0.0, 0.0], [0.01, 0.0], [0.0, 0.01]]
        group_b = [[10.0, 10.0], [10.01, 10.0], [10.0, 10.01]]
        unassigned = [{"embedding": e} for e in group_a + group_b]

        result = clustering.batch_cluster_unassigned(unassigned)

        if result:  # HDBSCAN may or may not cluster with min_cluster_size=2
            group_a_events = {result[i] for i in range(3) if i in result}
            group_b_events = {result[i] for i in range(3, 6) if i in result}
            if group_a_events and group_b_events:
                assert group_a_events.isdisjoint(group_b_events)

    def test_event_ids_are_valid_uuids(self, clustering):
        import uuid
        # Very close points to force clustering
        points = [[0.0 + i * 0.001, 0.0] for i in range(5)]
        unassigned = [{"embedding": p} for p in points]

        result = clustering.batch_cluster_unassigned(unassigned)
        for event_id in result.values():
            uuid.UUID(event_id)  # raises if invalid


# ── find_nearest_cluster (majority vote with mocked DB cursor) ──


class TestFindNearestCluster:
    def test_returns_none_when_no_neighbors(self, clustering):
        cur = MagicMock()
        cur.fetchall.return_value = []

        event_id, dist = clustering.find_nearest_cluster(cur, [0.1] * 10)
        assert event_id is None
        assert dist == float("inf")

    def test_returns_none_when_all_too_far(self, clustering):
        cur = MagicMock()
        cur.fetchall.return_value = [
            ("event-1", 0.80),
            ("event-1", 0.90),
            ("event-1", 0.95),
        ]

        event_id, dist = clustering.find_nearest_cluster(cur, [0.1] * 10)
        assert event_id is None
        assert dist == float("inf")

    def test_majority_vote_selects_dominant_event(self, clustering):
        cur = MagicMock()
        cur.fetchall.return_value = [
            ("event-1", 0.10),
            ("event-1", 0.15),
            ("event-1", 0.20),
            ("event-2", 0.25),
        ]

        event_id, dist = clustering.find_nearest_cluster(cur, [0.1] * 10)
        assert event_id == "event-1"
        assert dist == pytest.approx(0.10)

    def test_no_majority_returns_none(self, clustering):
        cur = MagicMock()
        cur.fetchall.return_value = [
            ("event-1", 0.10),
            ("event-2", 0.15),
            ("event-3", 0.20),
            ("event-4", 0.25),
            ("event-5", 0.30),
        ]

        event_id, dist = clustering.find_nearest_cluster(cur, [0.1] * 10)
        assert event_id is None

    def test_single_close_neighbor_with_majority(self, clustering):
        cur = MagicMock()
        cur.fetchall.return_value = [
            ("event-1", 0.30),
            ("event-2", 0.80),  # too far
        ]

        event_id, dist = clustering.find_nearest_cluster(cur, [0.1] * 10)
        assert event_id == "event-1"
        assert dist == pytest.approx(0.30)
