import numpy as np

from include.ml.clustering import cluster_embeddings


def test_cluster_embeddings_groups_close_points_separately_from_far_ones():
    # two tight groups of 3 points each, far apart -> expect 2 clusters, no assertion on labels order
    group_a = np.array([[0.0, 0.0], [0.01, 0.0], [0.0, 0.01]])
    group_b = np.array([[10.0, 10.0], [10.01, 10.0], [10.0, 10.01]])
    embeddings = np.vstack([group_a, group_b])
    article_ids = [f"a{i}" for i in range(len(embeddings))]

    assignments = cluster_embeddings(article_ids, embeddings)

    event_ids = {a.event_id for a in assignments}
    event_ids.discard(None)
    assert len(event_ids) == 2

    # points within the same original group should share an event_id
    a_events = {a.event_id for a in assignments[:3]}
    b_events = {a.event_id for a in assignments[3:]}
    assert len(a_events) == 1
    assert len(b_events) == 1
    assert a_events != b_events
