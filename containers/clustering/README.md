# Clustering Container

Reads new article embeddings from MinIO, assigns each article to an existing cluster or creates a new one using KNN + HDBSCAN, writes assignments back to MinIO and updates the database.

## What it does

### Phase 1 — KNN assignment
For each new article, queries pgvector for the 5 nearest neighbors in the database by L2 (euclidean) distance. If 60%+ of neighbors within distance 0.50 share the same cluster, the article is assigned to that cluster.

### Phase 2 — HDBSCAN on unassigned
Articles that didn't match any existing cluster are batch-clustered with HDBSCAN (`min_cluster_size=2`, `min_samples=1`, `metric=euclidean`, `cluster_selection_method=eom`) — same parameters used for the initial corpus clustering. This discovers new multi-article events from the batch.

### Phase 3 — Single-article events
Any articles still unassigned after HDBSCAN become their own single-article events.

All assignments are written to MinIO and the `articles` + `events` tables in the database are updated.

## I/O Contract

**Input** (from embedder): `embeddings/{RUN_DATE}/embeddings.ndjson`
```json
{"article_id": "...", "embedding": [0.024, -0.010, ...]}
```

**Output**: `clusters/{RUN_DATE}/cluster_assignments.ndjson`
```json
{"article_id": "...", "event_id": "uuid", "cluster_method": "knn", "distance": 0.32}
{"article_id": "...", "event_id": "uuid", "cluster_method": "hdbscan_new", "distance": 0.0}
{"article_id": "...", "event_id": "uuid", "cluster_method": "single", "distance": 0.0}
```

`cluster_method` indicates how the article was assigned:
- `knn` — matched an existing cluster via nearest neighbor vote
- `hdbscan_new` — grouped into a new cluster by HDBSCAN
- `single` — no match, became its own single-article event

## Distance threshold

Derived from the existing corpus of 1655 articles:

| Metric | Value |
|--------|-------|
| Average intra-cluster nearest neighbor distance | 0.43 |
| Median | 0.44 |
| P90 | 0.50 |
| P95 | 0.51 |

Threshold set to **0.50** (P90) — captures most true cluster members while avoiding false merges.

## Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `INPUT_KEY` | Yes | S3 key of embeddings NDJSON (e.g. `embeddings/2026-08-15/embeddings.ndjson`) |
| `STORAGE_ENDPOINT` | Yes | S3/MinIO endpoint |
| `STORAGE_BUCKET` | Yes | Bucket name |
| `AWS_ACCESS_KEY_ID` | Yes | S3/MinIO access key |
| `AWS_SECRET_ACCESS_KEY` | Yes | S3/MinIO secret key |
| `DB_HOST` | No | Database host (default: `news-db`) |
| `DB_PORT` | No | Database port (default: `5432`) |
| `DB_NAME` | No | Database name (default: `news_pipeline`) |
| `DB_USER` | No | Database user (default: `news`) |
| `DB_PASSWORD` | No | Database password (default: `news`) |
| `DISTANCE_THRESHOLD` | No | Max L2 distance for KNN assignment (default: `0.50`) |
| `K_NEIGHBORS` | No | Number of nearest neighbors to check (default: `5`) |
| `MAJORITY_RATIO` | No | Fraction of K that must agree on a cluster (default: `0.6`) |

## Build and run

```bash
# Build
docker build -t newslens/clustering:v1 .

# Run
docker run --rm --network newslens_pipeline_default --env-file ../../.env -e INPUT_KEY=embeddings/2026-08-15/embeddings.ndjson newslens/clustering:v1
```

## Database interaction

This container both reads and writes to the `news-db` database:

- **Reads**: queries `articles.embedding` via pgvector L2 index for nearest neighbor search
- **Writes**: inserts new rows into `events`, updates `articles.event_id` for assigned articles

The pgvector index (`idx_articles_embedding` using `ivfflat` with `vector_l2_ops`) must be analyzed after bulk inserts for optimal performance:
```sql
ANALYZE articles;
```

## Prerequisites

Articles must exist in the database with embeddings before clustering can assign new articles to existing clusters. The initial corpus was loaded via `include/db/load_corpus.py` (1655 articles, 348 clusters).
