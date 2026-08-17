# NewsLens Pipeline — Architecture and Operations Guide

Detailed reference for how the pipeline works, how to operate it, and
how to extend it. For quick start instructions, see the main `README.md`.

## Table of contents

1. [Architecture overview](#architecture-overview)
2. [Docker Compose services](#docker-compose-services)
3. [How the DAG works](#how-the-dag-works)
4. [Data flow and XCom](#data-flow-and-xcom)
5. [Credentials and secrets](#credentials-and-secrets)
6. [Container details](#container-details)
7. [Database schema](#database-schema)
8. [Adding a new scraper source](#adding-a-new-scraper-source)
9. [Swapping a pipeline stage](#swapping-a-pipeline-stage)
10. [Troubleshooting](#troubleshooting)

---

## Architecture overview

The pipeline has two layers:

**Infrastructure layer** — always-running Docker Compose services:
- Airflow scheduler + API server (orchestration)
- PostgreSQL for Airflow metadata
- PostgreSQL + pgvector for pipeline data (news-db)
- MinIO for intermediate file storage

**Pipeline layer** — short-lived Docker containers launched per task:
- Each pipeline stage (scrape, clean, embed, bias, cluster, load) is a
  separate Docker image with its own dependencies
- The scheduler launches these as sibling containers via the Docker socket
- Containers start, do their work, print their output key, and exit

### Why containers per stage

- **No dependency conflicts** — torch for embeddings, hdbscan for
  clustering, xgboost for bias classification — each image carries only
  what it needs
- **Independent swapping** — build a new image, update the image tag,
  next run uses it. No DAG changes, no Airflow rebuild
- **Slim Airflow image** — the scheduler/apiserver image is ~500MB
  instead of 5GB+ with all ML deps bundled
- **Instant rollback** — old images stay on disk, revert the tag to go
  back

### MinIO-first, DB-last

All pipeline stages write intermediate results to MinIO as NDJSON files.
Only the final loader stage writes to the database, in a single
transaction. This design gives us:

- **Debuggability** — inspect any stage's output by downloading the NDJSON
  from MinIO (`mc cat local/newslens-pipeline/cleaned/2026-08-17/articles.ndjson`)
- **Retry-friendly** — if the loader fails, all intermediate data is
  still in MinIO. Fix the issue and re-run just the loader
- **Atomic writes** — the database either gets all results from a run or
  none of them (single transaction)
- **Decoupled stages** — stages don't need database access (except
  clustering, which reads existing embeddings for KNN)

---

## Docker Compose services

Defined in `docker-compose.yaml`. All share the `newslens_pipeline_default`
network.

| Service | Image | Purpose | Ports |
|---------|-------|---------|-------|
| `postgres` | `postgres:16` | Airflow metadata DB (DAG runs, XCom, variables, connections) | 5432 (internal only) |
| `airflow-apiserver` | Built from `./Dockerfile` | Web UI + REST API | `localhost:8080` |
| `airflow-scheduler` | Built from `./Dockerfile` | Parses DAGs, schedules tasks, launches containers via Docker socket | — |
| `airflow-init` | Built from `./Dockerfile` | One-shot: DB migrations, creates admin user, then exits | — |
| `news-db` | `pgvector/pgvector:pg16` | Pipeline data warehouse (articles, embeddings, events) | `localhost:5433` |
| `minio` | `minio/minio:latest` | Local S3 replacement for intermediate NDJSON files | `localhost:9000` (API), `localhost:9001` (console) |

### Key volume mounts

The scheduler has two important mounts:

```yaml
- ./dags:/opt/airflow/dags          # DAG files, auto-detected on change
- /var/run/docker.sock:/var/run/docker.sock  # Docker socket for launching containers
```

The Docker socket mount is what allows the scheduler (running inside a
container) to launch sibling containers on the host. The scheduler's
`group_add: ["984"]` gives the Airflow user permission to use the socket
(984 is the host's `docker` group GID).

### Airflow image

The `Dockerfile` in the project root builds the Airflow image:

```dockerfile
FROM apache/airflow:3.3.1-python3.11
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
```

This only installs `apache-airflow-providers-docker` and lightweight
deps — no ML libraries. The scheduler uses this image to parse DAGs and
call the Docker API, nothing more.

---

## How the DAG works

File: `dags/news_pipeline_dag.py`

### Task graph

```
scrape → clean → embed → bias    \
                       → cluster  → load
```

Each task is a `DockerOperator` that:
1. Launches a container from a specific image (e.g., `newslens/scraper:v1`)
2. Passes environment variables (MinIO creds + task-specific input keys)
3. Waits for the container to exit
4. Captures the last line of stdout as the XCom return value
5. Removes the container after success

### Shared configuration

Two dicts are defined at module level:

**`COMMON_ENV`** — loaded from the Airflow Variable `pipeline_env` (a
JSON dict). Contains MinIO credentials, DB credentials, Modal tokens,
and model paths. Passed to every container via the `environment`
parameter.

**`DOCKER_DEFAULTS`** — shared DockerOperator settings:
- `network_mode`: `newslens_pipeline_default` (so containers can reach
  MinIO and news-db by hostname)
- `auto_remove`: `success` (clean up containers after successful exit)
- `docker_url`: `unix://var/run/docker.sock`
- `mount_tmp_dir`: `False` (containers use MinIO, not local temp dirs)

### Task definitions

Each task merges `COMMON_ENV` with its own environment variables:

```python
clean = DockerOperator(
    task_id="clean",
    image="newslens/cleaner:v1",
    environment={**COMMON_ENV, "INPUT_KEY": "{{ ti.xcom_pull(task_ids='scrape') }}"},
    do_xcom_push=True,
    **DOCKER_DEFAULTS,
)
```

The `{{ ti.xcom_pull(task_ids='scrape') }}` is a Jinja template that
Airflow resolves before the container starts — replacing it with the
actual string the scraper printed (e.g., `raw/2026-08-17/articles.ndjson`).

### Parallel tasks

Bias classification and clustering both depend only on embeddings output.
The DAG expresses this as:

```python
embed >> [bias, cluster] >> load
```

Airflow runs `bias` and `cluster` simultaneously. The `load` task waits
for both to finish.

### Schedule

Currently set to `schedule=None` (manual trigger only). Change to
`schedule="0 */4 * * *"` for automatic runs every 4 hours.

---

## Data flow and XCom

### What is XCom

XCom ("cross-communication") is Airflow's key-value store for passing
small pieces of data between tasks. It's stored in the Airflow metadata
database (the `postgres` service, not `news-db`).

### How data flows

```
scrape container
  └─ writes to MinIO: raw/2026-08-17/articles.ndjson
  └─ prints to stdout: "raw/2026-08-17/articles.ndjson"
  └─ DockerOperator captures stdout → stores in XCom

clean container
  └─ receives INPUT_KEY="raw/2026-08-17/articles.ndjson" (from XCom)
  └─ reads from MinIO: raw/2026-08-17/articles.ndjson
  └─ writes to MinIO: cleaned/2026-08-17/articles.ndjson
  └─ prints to stdout: "cleaned/2026-08-17/articles.ndjson"

... and so on through embed → bias/cluster → load
```

The actual article data (potentially megabytes) flows through MinIO.
Only the tiny storage key string (a few bytes) flows through XCom.

### The loader is special

The loader needs outputs from four upstream tasks. It pulls each one
separately:

```python
environment={
    **COMMON_ENV,
    "CLEAN_KEY": "{{ ti.xcom_pull(task_ids='clean') }}",
    "EMBEDDINGS_KEY": "{{ ti.xcom_pull(task_ids='embed') }}",
    "BIAS_KEY": "{{ ti.xcom_pull(task_ids='bias') }}",
    "CLUSTERS_KEY": "{{ ti.xcom_pull(task_ids='cluster') }}",
}
```

---

## Credentials and secrets

### Where credentials live

| Credential | Stored in | Used by |
|------------|-----------|---------|
| Airflow DB password | `.env` → `docker-compose.yaml` | Airflow services only |
| Fernet key | `.env` → `docker-compose.yaml` | Airflow (encrypts Variables/Connections) |
| MinIO keys | Airflow Variable `pipeline_env` | Pipeline containers |
| News DB password | Airflow Variable `pipeline_env` | Pipeline containers |
| Modal tokens | Airflow Variable `pipeline_env` | Embedder container |
| Bias model path | Airflow Variable `pipeline_env` | Bias classifier container |

### Why Airflow Variables

The `.env` file only contains credentials needed by docker-compose
services (Airflow's own DB, MinIO root user). Container credentials
are stored in an Airflow Variable called `pipeline_env` — a JSON dict
encrypted at rest with the Fernet key.

This keeps secrets out of version-controlled code. The DAG file only
references `Variable.get("pipeline_env")`.

### Viewing and updating

```bash
# View current values
docker exec newslens_pipeline-airflow-scheduler-1 \
  airflow variables get pipeline_env

# Update (overwrites entire JSON)
docker exec newslens_pipeline-airflow-scheduler-1 \
  airflow variables set pipeline_env '{ ... }'
```

Or via the Airflow UI: Admin → Variables.

### The pipeline_env variable

```json
{
  "STORAGE_ENDPOINT": "http://minio:9000",
  "STORAGE_BUCKET": "newslens-pipeline",
  "AWS_ACCESS_KEY_ID": "minioadmin",
  "AWS_SECRET_ACCESS_KEY": "minioadmin",
  "DB_HOST": "news-db",
  "DB_PORT": "5432",
  "DB_NAME": "news_pipeline",
  "DB_USER": "news",
  "DB_PASSWORD": "news",
  "USE_MODAL": "true",
  "MODEL_KEY": "models/bias-classifier-xgb/xgb_model.joblib",
  "MODAL_TOKEN_ID": "<your-token-id>",
  "MODAL_TOKEN_SECRET": "<your-token-secret>"
}
```

---

## Container details

Each container follows the same pattern:
1. Read `INPUT_KEY` (or stage-specific keys) from environment
2. Download input NDJSON from MinIO
3. Process it
4. Upload output NDJSON to MinIO
5. Print the output key to stdout (for XCom)

### Scraper (`newslens/scraper:v1`)

- **What:** Crawls Sinhala news sites using the `lk_news` package
- **Sources:** DivainaLk (working, ~36 articles per run)
- **Input:** None (it's the first stage). Uses `RUN_DATE` env var
- **Output:** `raw/{date}/articles.ndjson`
- **Fields:** `article_id, source, url, title, body, language, published_at, scraped_at`
- **Details:** `containers/scraper/README.md`

### Cleaner (`newslens/cleaner:v1`)

- **What:** Unicode normalization, zero-width character removal, HTML
  entity cleanup, deduplication, length filtering
- **Input:** `INPUT_KEY` → raw articles NDJSON
- **Output:** `cleaned/{date}/articles.ndjson`
- **Fields:** Same as scraper, text normalized
- **Notable:** Removes zero-width joiners common in Sinhala Unicode text
- **Details:** `containers/cleaner/README.md`

### Embedder (`newslens/embedder:v1`)

- **What:** Generates 1024-dim embeddings using `intfloat/multilingual-e5-large`
- **Inference:** Modal remote GPU (`USE_MODAL=true`) or local GPU
- **Input:** `INPUT_KEY` → cleaned articles NDJSON
- **Output:** `embeddings/{date}/embeddings.ndjson`
- **Fields:** `article_id, embedding` (1024-dim float array)
- **Details:** `containers/embedder/README.md`

### Bias classifier (`newslens/bias-classifier-xgb:v1`)

- **What:** XGBoost classifier on pre-computed E5 embeddings
- **Labels:** `far_left, left, center, right, far_right`
- **Model:** Downloaded from MinIO at `MODEL_KEY` path
- **Input:** `INPUT_KEY` → embeddings NDJSON
- **Output:** `bias/{date}/bias_results.ndjson`
- **Fields:** `article_id, bias_label, bias_confidence, bias_scores`
- **Alternative:** `bias-classifier-transformers/` (HelaBERT, untested)
- **Details:** `containers/bias-classifier-xgb/README.md`

### Clustering (`newslens/clustering:v1`)

- **What:** Three-phase clustering:
  1. KNN against existing DB embeddings (pgvector L2 distance)
  2. HDBSCAN on unassigned articles (new clusters)
  3. Single-article events for remaining unassigned
- **Parameters:** distance threshold 0.50, K=5, majority ratio 0.6,
  HDBSCAN min_cluster_size=2
- **Input:** `INPUT_KEY` → embeddings NDJSON
- **Output:** `clusters/{date}/cluster_assignments.ndjson`
- **Fields:** `article_id, event_id, cluster_method, distance`
- **Note:** This is the only stage that reads from the database
  (existing embeddings for KNN neighbors)
- **Details:** `containers/clustering/README.md`

### Loader (`newslens/loader:v1`)

- **What:** Reads all 4 NDJSON outputs, joins by article_id, upserts to
  database in a single transaction
- **Input:** `CLEAN_KEY`, `EMBEDDINGS_KEY`, `BIAS_KEY`, `CLUSTERS_KEY`
- **Output:** Prints `done` to stdout
- **What it writes:** sources table, events table (with article counts),
  articles table (all fields including embedding vector)
- **Details:** `containers/loader/README.md`

---

## Database schema

Database: `news_pipeline` on the `news-db` service.
Connection: `psql -h localhost -p 5433 -U news -d news_pipeline`

Three tables managed by SQLAlchemy models in `include/db/models.py`:

### sources

| Column | Type | Description |
|--------|------|-------------|
| source_id | UUID (PK) | Auto-generated |
| source_name | TEXT (unique) | e.g., "DivainaLk" |

### articles

| Column | Type | Description |
|--------|------|-------------|
| article_id | UUID (PK) | Generated by scraper |
| source_id | UUID (FK → sources) | |
| event_id | UUID (FK → events, nullable) | Cluster assignment |
| url | TEXT | Original article URL |
| title | TEXT | |
| body | TEXT | Cleaned body text |
| language | TEXT | e.g., "si" |
| embedding | VECTOR(1024) | E5-large embedding |
| bias_label | TEXT | far_left/left/center/right/far_right |
| bias_confidence | FLOAT | Model confidence score |
| published_at | TIMESTAMP | When the article was published |
| scraped_at | TIMESTAMP (nullable) | When we scraped it |
| summary | TEXT (nullable) | On-demand, not set by pipeline |
| topic | TEXT (nullable) | On-demand, not set by pipeline |

### events

| Column | Type | Description |
|--------|------|-------------|
| event_id | UUID (PK) | Generated by clustering |
| article_count | INTEGER | Number of articles in this cluster |

Migrations are managed by Alembic in `include/db/migrations/`.

---

## Adding a new scraper source

The scraper uses the `lk_news` package's `AbstractNewsPaper` pattern.
To add a new source:

1. Check available sources in the `lk_news` package
2. Add the source class to `containers/scraper/run.py`
3. Rebuild: `docker build -t newslens/scraper:v1 containers/scraper/`
4. Next pipeline run will include articles from the new source

See `containers/scraper/README.md` for details on the scraping mechanics.

---

## Swapping a pipeline stage

The I/O contract (NDJSON field names) is what makes swapping possible.
As long as a new container reads and writes the same fields, it's a
valid replacement.

### Example: swapping bias classifier from XGBoost to HelaBERT

1. Build the new image:
   ```bash
   docker build -t newslens/bias-classifier-transformers:v1 \
     containers/bias-classifier-transformers/
   ```

2. Update the image name in the DAG (`dags/news_pipeline_dag.py`):
   ```python
   bias = DockerOperator(
       task_id="bias",
       image="newslens/bias-classifier-transformers:v1",  # changed
       ...
   )
   ```

3. The scheduler picks up the DAG change automatically (within ~30s).
   Next run uses the new container.

4. To rollback, revert the image name in the DAG.

---

## Troubleshooting

### "Permission denied" on Docker socket

The scheduler needs the host's Docker group GID in `group_add`.
Check and fix:

```bash
# Find the Docker group GID on your host:
getent group docker | cut -d: -f3

# Update docker-compose.yaml scheduler service:
group_add:
  - "<GID>"  # e.g., "984"

# Recreate the scheduler:
docker compose up -d airflow-scheduler
```

### "database does not exist" from news-db healthcheck

The `.env` `NEWS_DB_NAME` must match the actual database name in the
existing volume. If the volume was initialized with a different name
(e.g., `news_pipeline`), update `.env` to match — don't delete the
volume (you'll lose data).

```bash
# Check actual database name:
docker exec newslens_pipeline-news-db-1 \
  psql -U news -d news_pipeline -c '\l'
```

### Container fails with "Token missing" (Modal auth)

The embedder needs Modal tokens. Add them to the `pipeline_env` Airflow
Variable:

```bash
# Generate tokens:
modal token new

# Read them:
cat ~/.modal.toml

# Add MODAL_TOKEN_ID and MODAL_TOKEN_SECRET to the pipeline_env variable
```

### DAG not appearing in Airflow UI

```bash
# Check for parse errors:
docker exec newslens_pipeline-airflow-scheduler-1 \
  airflow dags reserialize

# Force refresh:
docker exec newslens_pipeline-airflow-scheduler-1 \
  airflow dags list
```

### Viewing intermediate data

```bash
# Install MinIO client
mc alias set local http://localhost:9000 minioadmin minioadmin

# List all pipeline outputs
mc ls --recursive local/newslens-pipeline/

# View a specific file
mc cat local/newslens-pipeline/cleaned/2026-08-17/articles.ndjson | head -1 | python -m json.tool
```

### Connecting to the database

```bash
# From your host machine:
psql -h localhost -p 5433 -U news -d news_pipeline

# From inside a container on the compose network:
psql -h news-db -p 5432 -U news -d news_pipeline

# Quick article count:
docker exec newslens_pipeline-news-db-1 \
  psql -U news -d news_pipeline -c "SELECT COUNT(*) FROM articles;"
```
