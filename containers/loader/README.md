# Loader Container

Final pipeline stage. Reads all intermediate NDJSON outputs from MinIO and writes everything to the news-db database in a single transaction.

## How data flows through the pipeline

Each stage reads from MinIO, processes, and writes back to MinIO. The loader collects all outputs at the end.

```
scrape ──→ raw/{date}/articles.ndjson
              │
clean  ──→ cleaned/{date}/articles.ndjson
              │
embed  ──→ embeddings/{date}/embeddings.ndjson
              │
bias   ──→ bias/{date}/bias_results.ndjson
              │
cluster ─→ clusters/{date}/cluster_assignments.ndjson
              │
loader  ──→ Reads all 4 NDJSON files above
              │
           news-db (sources + events + articles)
```

### Step by step

1. **Scraper** scrapes news sites → writes raw articles NDJSON to MinIO
2. **Cleaner** reads raw → normalizes text, deduplicates → writes cleaned NDJSON to MinIO
3. **Embedder** reads cleaned → generates 1024-dim E5 embeddings → writes embeddings NDJSON to MinIO
4. **Bias classifier** reads embeddings (XGBoost) or cleaned articles (HelaBERT) → classifies → writes bias results NDJSON to MinIO
5. **Clustering** reads embeddings, queries DB for nearest neighbors → assigns clusters → writes cluster assignments NDJSON to MinIO
6. **Loader** reads cleaned articles + embeddings + bias results + cluster assignments from MinIO → joins everything by `article_id` → inserts into database in one transaction

## What the loader does

1. **Reads** all 4 NDJSON files from MinIO
2. **Indexes** embeddings, bias results, and cluster assignments by `article_id`
3. **Upserts sources** into the `sources` table (deduped by `source_name`)
4. **Upserts events** into the `events` table with article and source counts. For existing events (KNN-assigned), increments `article_count`
5. **Upserts articles** into the `articles` table with all fields joined: title, body, embedding, bias label, event assignment
6. **Runs `ANALYZE`** on the articles table to keep the pgvector index fresh for the next clustering run

All writes happen in a single transaction — if anything fails, nothing is committed.

## I/O Contract

**Input** (from MinIO):
- `cleaned/{date}/articles.ndjson` — `{article_id, source, url, title, body, language, published_at, scraped_at}`
- `embeddings/{date}/embeddings.ndjson` — `{article_id, embedding}`
- `bias/{date}/bias_results.ndjson` — `{article_id, bias_label, bias_confidence, bias_scores}`
- `clusters/{date}/cluster_assignments.ndjson` — `{article_id, event_id, cluster_method, distance}`

**Output**: writes to `news-db` tables (`sources`, `events`, `articles`). Prints `done` to stdout.

## Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `CLEAN_KEY` | Yes | S3 key for cleaned articles NDJSON |
| `EMBEDDINGS_KEY` | Yes | S3 key for embeddings NDJSON |
| `BIAS_KEY` | Yes | S3 key for bias results NDJSON |
| `CLUSTERS_KEY` | Yes | S3 key for cluster assignments NDJSON |
| `STORAGE_ENDPOINT` | Yes | S3/MinIO endpoint |
| `STORAGE_BUCKET` | Yes | Bucket name |
| `AWS_ACCESS_KEY_ID` | Yes | S3/MinIO access key |
| `AWS_SECRET_ACCESS_KEY` | Yes | S3/MinIO secret key |
| `DB_HOST` | No | Database host (default: `news-db`) |
| `DB_PORT` | No | Database port (default: `5432`) |
| `DB_NAME` | No | Database name (default: `news_pipeline`) |
| `DB_USER` | No | Database user (default: `news`) |
| `DB_PASSWORD` | No | Database password (default: `news`) |

## Build and run

```bash
# Build
docker build -t newslens/loader:v1 .

# Run
docker run --rm --network newslens_pipeline_default --env-file ../../.env \
  -e CLEAN_KEY=cleaned/2026-08-15/articles.ndjson \
  -e EMBEDDINGS_KEY=embeddings/2026-08-15/embeddings.ndjson \
  -e BIAS_KEY=bias/2026-08-15/bias_results.ndjson \
  -e CLUSTERS_KEY=clusters/2026-08-15/cluster_assignments.ndjson \
  newslens/loader:v1
```

## Why MinIO-first, DB-last

### Pros

- **Retry-friendly**: If any stage fails, its MinIO input is still there. Re-run just that stage without re-running everything before it.
- **Debuggable**: Every intermediate result is inspectable as a plain NDJSON file. You can read, diff, or manually fix any stage's output.
- **Decoupled**: Stages don't depend on the database schema. Only the loader needs to know how to map NDJSON fields to SQL columns. Changing the schema means changing one container, not six.
- **Atomic DB writes**: All data enters the database in one transaction. No partial state — either the full pipeline run is committed or nothing is.
- **Swappable stages**: Replacing a stage (e.g., swapping bias classifier) only requires matching the NDJSON contract. The loader doesn't care which container produced the data.

### Cons

- **Storage duplication**: Data exists in both MinIO (NDJSON) and the database. For 36 articles this is negligible, but at scale the embeddings NDJSON (1024 floats × N articles) can grow large.
- **Extra latency**: The loader must re-read all NDJSON files at the end instead of writing to the DB as each stage completes.
- **Stale DB during pipeline run**: The database doesn't have today's articles until the loader finishes. If the pipeline fails before the loader, today's articles are only in MinIO.
- **Clustering reads DB**: The clustering container still queries the database for nearest neighbors, so it depends on previous runs having completed the full pipeline (including the loader). This is the one stage that breaks pure MinIO isolation.
