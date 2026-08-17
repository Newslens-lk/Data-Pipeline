# NewsLens.lk Data-Pipeline

Sinhala news aggregation and analysis pipeline. Scrapes articles from
multiple sources, normalizes text, generates embeddings, classifies
political bias, clusters articles into events, and loads everything to
Postgres + pgvector.

Summarization and topic tagging are on-demand (API layer), not pipeline
stages.

## Pipeline

```
scrape → clean → embed → bias-classify  → load to DB
                       → cluster         →
```

Bias classification and clustering run in parallel — both only need
embeddings. The loader waits for all upstream stages, then writes
everything to the database in a single transaction.

## Architecture

**Apache Airflow** orchestrates the pipeline. Each stage runs in its own
**Docker container**, launched by the scheduler via `DockerOperator`.
Airflow does not run any stage logic itself — it launches containers,
passes MinIO storage keys between them (via XCom), and handles retries.

```
┌─────────────────────────────────────────────────────────┐
│  Docker Host                                            │
│                                                         │
│  Compose services (always running):                     │
│  ┌────────────┐ ┌───────────┐ ┌────────┐ ┌───────────┐ │
│  │ scheduler  │ │ apiserver │ │ MinIO  │ │  news-db  │ │
│  │            │ │  :8080    │ │:9000/01│ │   :5433   │ │
│  └─────┬──────┘ └───────────┘ └────────┘ └───────────┘ │
│        │                                                │
│        │ docker.sock (launches sibling containers)      │
│        ▼                                                │
│  Per-task containers (start, run, exit):                 │
│  ┌─────────┐ ┌─────────┐ ┌──────────┐ ┌──────┐        │
│  │ scraper │ │ cleaner │ │ embedder │ │ bias │ ...     │
│  └─────────┘ └─────────┘ └──────────┘ └──────┘        │
└─────────────────────────────────────────────────────────┘
```

### How stages communicate

Every container reads NDJSON from MinIO, writes NDJSON back, and prints
the output storage key to stdout. Airflow captures that last line via
XCom and passes it to the next stage as an environment variable.

The data never flows through Airflow — only the tiny string key does.

### Data stores

| Store | Purpose | Access |
|-------|---------|--------|
| **MinIO** (local S3) | Intermediate NDJSON between stages | `http://minio:9000` from containers, `http://localhost:9000` from host |
| **news-db** (Postgres + pgvector) | Final articles, embeddings, events | `news-db:5432` from containers, `localhost:5433` from host |
| **postgres** | Airflow metadata (DAG runs, XCom, variables) | Internal to Airflow, not accessed directly |

## Repo layout

```
newslens_pipeline/
├── dags/
│   └── news_pipeline_dag.py    # DAG definition (DockerOperator)
├── containers/                 # One directory per pipeline stage
│   ├── scraper/                #   each has: Dockerfile, run.py,
│   ├── cleaner/                #   requirements.txt, README.md
│   ├── embedder/
│   ├── bias-classifier-xgb/
│   ├── bias-classifier-transformers/  # HelaBERT alternative (untested)
│   ├── clustering/
│   └── loader/
├── include/                    # Shared code mounted into Airflow
│   └── db/
│       ├── models.py           # SQLAlchemy models (Source, Article, Event)
│       ├── load_corpus.py      # One-time Colab corpus loader
│       └── migrations/         # Alembic schema migrations
├── config/                     # Airflow config
├── docs/
│   └── Guide.md                # Detailed architecture and operations guide
├── infra/
│   └── README.md               # Cloud deployment notes (MWAA/Composer/ADF)
├── docker-compose.yaml         # 6 services: postgres, apiserver, scheduler,
│                               #   airflow-init, news-db, minio
├── Dockerfile                  # Airflow image (slim, no ML deps)
├── requirements.txt            # Airflow Python deps only
└── .env                        # Credentials (gitignored)
```

## Quick start (reproduce from scratch)

### Prerequisites

- Docker and Docker Compose
- A Modal account (for remote GPU embeddings) — `pip install modal && modal token new`

### 1. Clone and configure

```bash
git clone <repo-url>
cd newslens_pipeline

# Create .env from this template:
cat > .env << 'EOF'
# Airflow
AIRFLOW_UID=1000
FERNET_KEY=<generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())">
AIRFLOW_DB_USER=db_user
AIRFLOW_DB_PASSWORD=db_password
AIRFLOW_DB_NAME=db_name

# MinIO
MINIO_ROOT_USER=miniIO_user
MINIO_ROOT_PASSWORD=miniIO_password

# News DB
NEWS_DB_USER=news_db_user
NEWS_DB_PASSWORD=news_db_password
NEWS_DB_NAME=news_db
EOF
```

### 2. Start infrastructure

```bash
docker compose up -d
# Wait for all services to be healthy:
docker compose ps
```

Airflow UI: http://localhost:8080 (login: `airflow` / `airflow`)
MinIO console: http://localhost:9001 (login: `minioadmin` / `minioadmin`)

### 3. Build pipeline containers

```bash
docker build -t newslens/scraper:v1 containers/scraper/
docker build -t newslens/cleaner:v1 containers/cleaner/
docker build -t newslens/embedder:v1 containers/embedder/
docker build -t newslens/bias-classifier-xgb:v1 containers/bias-classifier-xgb/
docker build -t newslens/clustering:v1 containers/clustering/
docker build -t newslens/loader:v1 containers/loader/
```

### 4. Create the MinIO bucket and upload the bias model

```bash
# Install MinIO client (mc) if not already installed
# Create the pipeline bucket:
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb local/newslens-pipeline

# Upload the XGBoost bias model:
mc cp containers/bias-classifier-xgb/xgboost_model.joblib \
     local/newslens-pipeline/models/bias-classifier-xgb/xgb_model.joblib
```

### 5. Set Airflow Variables (secrets)

This stores container credentials in Airflow's encrypted metadata DB,
keeping them out of version-controlled code.

```bash
docker exec newslens_pipeline-airflow-scheduler-1 \
  airflow variables set pipeline_env '{
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
    "MODAL_TOKEN_ID": "<your-modal-token-id>",
    "MODAL_TOKEN_SECRET": "<your-modal-token-secret>"
  }'
```

### 6. Run the pipeline

1. Go to http://localhost:8080
2. Find `news_event_pipeline` in the DAG list
3. Toggle the pause switch to unpause
4. Click the play button to trigger a run
5. Watch tasks go green in the graph view

### 7. Verify results

```bash
# Check articles in the database:
docker exec -it newslens_pipeline-news-db-1 \
  psql -U news -d news_pipeline -c "SELECT COUNT(*) FROM articles;"

# Check MinIO for intermediate files:
mc ls local/newslens-pipeline/raw/
mc ls local/newslens-pipeline/cleaned/
mc ls local/newslens-pipeline/embeddings/
mc ls local/newslens-pipeline/bias/
mc ls local/newslens-pipeline/clusters/
```

## I/O contracts

Each container reads and writes NDJSON (one JSON object per line) via MinIO.

| Stage | Input key | Output key | Fields |
|-------|-----------|------------|--------|
| Scraper | — | `raw/{date}/articles.ndjson` | `article_id, source, url, title, body, language, published_at, scraped_at` |
| Cleaner | `raw/{date}/articles.ndjson` | `cleaned/{date}/articles.ndjson` | Same fields, deduplicated and normalized |
| Embedder | `cleaned/{date}/articles.ndjson` | `embeddings/{date}/embeddings.ndjson` | `article_id, embedding` (1024-dim float array) |
| Bias classifier | `embeddings/{date}/embeddings.ndjson` | `bias/{date}/bias_results.ndjson` | `article_id, bias_label, bias_confidence, bias_scores` |
| Clustering | `embeddings/{date}/embeddings.ndjson` | `clusters/{date}/cluster_assignments.ndjson` | `article_id, event_id, cluster_method, distance` |
| Loader | All 4 above | — (writes to DB) | Prints `done` |

## Swapping a stage

```bash
# 1. Build new image
docker build -t newslens/bias-classifier-xgb:v2 containers/bias-classifier-xgb/

# 2. Update the image tag in the DAG (or use Airflow Variables for image names)

# 3. Next run uses the new container
# To rollback: rebuild with the old tag or revert the image reference
```

See `docs/Guide.md` for detailed architecture and operations documentation.
See each container's `README.md` for stage-specific details.
