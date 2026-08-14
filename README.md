# NewsLens Pipeline

Airflow-orchestrated news pipeline with swappable containers. Scrapes
articles, classifies bias, generates embeddings, clusters into events,
summarizes and topic-tags each event, then loads everything to Postgres + pgvector.

## Architecture

The pipeline splits into two layers:

1. **Orchestration layer** (Airflow) — DAG wiring, cleaning, and database
   loading. Runs inside the Airflow worker. Lightweight, changes rarely.

2. **Containerized stages** — scraping, bias classification, embedding,
   clustering, summarization. Each stage runs in its own Docker container,
   independently versioned and deployable.

```
Airflow Worker (slim image)          Containers (one per stage)
┌──────────────────────────┐         ┌─────────────────────────┐
│                    ──────┼────────>│  scraper:v1             │
│  clean  <────────────────┼─────────│                         │
│         ─────────────────┼────────>│  bias-classifier:v2     │
│         ─────────────────┼────────>│  embedder:v1            │
│                          │<────────│                         │
│                          │         └─────────────────────────┘
│                          │         ┌─────────────────────────┐
│              cluster  <──┼─────────│  clustering:v1          │
│              summarize <─┼─────────│  summarizer:v3          │
│                          │         └─────────────────────────┘
│  load (to Postgres)      │
└──────────────────────────┘
```

### Why this split?

**Dependency isolation.** The bias classifier needs torch + transformers (~2GB).
The embedder needs sentence-transformers. Clustering needs hdbscan. The
summarizer just needs the anthropic SDK. The scraper may evolve to need
playwright, scrapy, or other heavy crawling frameworks. Bundling all of these
into one Airflow worker image creates a 5GB+ image where version conflicts
between frameworks are a constant risk. Per-stage containers eliminate this
entirely — each image carries only its own deps. This directly addresses what
Sculley et al. call "dependency debt" in *Hidden Technical Debt in Machine
Learning Systems* (NeurIPS 2015).

**Independent swapping.** When you improve any stage — a better bias
classifier, a more robust scraper, a different embedding model — you build a
new container image, tag it (`bias-classifier:v3`, `scraper:v2`), and update a
single Airflow Variable. The next DAG run picks up the new container. No DAG
code changes, no redeployment of the Airflow environment, no risk of breaking
unrelated stages. This is the microservices principle of independently
deployable components — swap one part without touching the rest, as long as the
I/O contract holds.

**Scraping is swappable too.** Scraping logic changes frequently: new sources
get added, sites change their markup, you may switch from RSS + BeautifulSoup
to Scrapy or Playwright for JS-heavy sites. Containerizing the scraper means
you can swap in a completely different crawling stack without touching the
Airflow image or any downstream stage. The contract is simple: produce NDJSON
with `{article_id, source, url, html, scraped_at}` — how you get there is the
container's business.

**Experimentation-production decoupling.** You experiment locally (notebooks,
scripts, whatever works) without touching Airflow. When something beats the
baseline, you containerize it and promote it. This maps to Level 2 of Google's
MLOps maturity model: the artifact (container) has its own lifecycle separate
from the orchestration code.

**Rollback in seconds.** Old images stay in the registry. If a new model or
scraper degrades quality, revert the Airflow Variable to the previous image
tag. The next run uses the old container. No code changes, no redeployment.
This is blue-green deployment applied to pipeline stages, a standard pattern
from Humble & Farley's *Continuous Delivery*.

**Slim Airflow image.** With ML and scraping deps removed, the Airflow worker
image drops from ~5GB to a few hundred MB. Faster builds, faster deploys,
faster scheduler startup. The DAG file stays cheap to parse since it imports
nothing heavy.

## I/O Contracts

The `contracts/` directory contains JSON Schema definitions for every
inter-stage data format. These are the invariant that makes swapping possible.

Every container follows the same pattern:
- **Input:** reads NDJSON from object storage (key passed via env var)
- **Output:** writes NDJSON to object storage, prints the output key to stdout
- **Schema:** must match the contract for its stage

As long as a new container honors the same input/output schema as the one it
replaces, it is a valid swap. This is contract-based integration — the same
principle that lets you swap database implementations behind a repository
interface, applied to pipeline stage containers.

| Stage | Input Schema | Output Schema |
|-------|-------------|---------------|
| scraper | sources config (YAML) | raw_article.json |
| bias-classifier | clean_article.json | bias_result.json |
| embedder | clean_article.json | embedding_result.json |
| clustering | embedding_result.json | cluster_assignment.json |
| summarizer | clean_article.json + cluster_assignment.json | event_summary.json + event_topic.json |

## The Experiment-to-Deploy Loop

1. **Experiment locally** — use saved sample data or the golden test set in
   `eval/`. Try new models, scraping strategies, architectures, hyperparameters.
   No Airflow needed.

2. **Evaluate against baseline** — compare metrics (F1, cosine similarity,
   human eval, scrape coverage) against the current production version's
   outputs on the same test set.

3. **Containerize** — if it beats the baseline, write a `run.py` that follows
   the I/O contract, build a Docker image, tag it with a version
   (`bias-classifier:v3`, `scraper:v2`), push to your registry.

4. **Swap in** — update the Airflow Variable (e.g. `bias_classifier_image`,
   `scraper_image`) to point at the new tag. Next DAG run uses it automatically.

5. **Monitor** — compare production outputs to previous runs. If regression,
   revert the Variable to the previous tag.

This is the strangler fig pattern (Fowler) applied incrementally — replace one
stage at a time while the rest of the pipeline continues running unchanged.

## Swapping a container

```bash
# 1. Build and tag
docker build -t myregistry/bias-classifier:v3 containers/bias-classifier/
docker push myregistry/bias-classifier:v3

# 2. Update Airflow Variable (UI, CLI, or API)
airflow variables set bias_classifier_image myregistry/bias-classifier:v3

# 3. Done — next DAG run uses v3
# To rollback:
airflow variables set bias_classifier_image myregistry/bias-classifier:v2
```

## Pipeline stages

1. **Scrape** — pull articles from configured RSS/HTML sources, land raw
   HTML as NDJSON in object storage. Runs in container.
2. **Clean** — strip boilerplate, deduplicate, language-filter, normalize.
   Runs in Airflow worker (lightweight, stable logic).
3. **Bias classify** — run fine-tuned classifier. Runs in container.
4. **Embed** — generate sentence embeddings. Runs in container.
5. **Cluster** — HDBSCAN over embeddings to group articles into events.
   Runs in container.
6. **Summarize + topic** — LLM summarization per event cluster + topic
   assignment. Runs in container.
7. **Load** — join all outputs, upsert to Postgres + pgvector. Runs in
   Airflow worker.

## Repo layout

```
newslens_pipeline/
├── dags/                        # DAG definitions (orchestration only)
│   └── news_pipeline_dag.py
├── containers/                  # Swappable stage containers
│   ├── scraper/                 #   each with: Dockerfile, requirements.txt,
│   ├── bias-classifier/         #   run.py, schemas.py
│   ├── embedder/
│   ├── clustering/
│   └── summarizer/
├── contracts/                   # JSON Schema definitions for inter-stage data
│   ├── raw_article.json
│   ├── clean_article.json
│   ├── bias_result.json
│   ├── embedding_result.json
│   ├── cluster_assignment.json
│   ├── event_summary.json
│   └── event_topic.json
├── include/                     # Non-containerized pipeline logic (Airflow worker)
│   ├── config.py
│   ├── preprocessing/
│   └── db/
├── eval/                        # Golden test sets for evaluation
├── tests/
├── infra/                       # Cloud deployment notes (MWAA/Composer/ADF)
├── plugins/
├── docker-compose.yaml
├── Dockerfile                   # Slim Airflow image (no ML/scraping deps)
└── requirements.txt             # Slim (no torch/transformers/hdbscan/bs4)
```

## Getting started locally

```bash
cp .env.example .env
docker compose up -d
# Airflow UI: http://localhost:8080 (admin/admin)

# Build stage containers locally
for stage in scraper bias-classifier embedder clustering summarizer; do
  docker build -t newslens/${stage}:dev containers/${stage}/
done
```

## Deploying to managed Airflow

See `infra/README.md`. The main addition vs. a monolithic setup: push container
images to your cloud's container registry (ECR / Artifact Registry / ACR) and
switch the DAG from `DockerOperator` to `KubernetesPodOperator` if running on
K8s.
