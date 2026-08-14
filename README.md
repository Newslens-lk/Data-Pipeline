# NewsLens Pipeline

News aggregation and analysis pipeline that scrapes articles from multiple
sources, detects political bias, groups articles about the same event, and
produces neutral multi-source summaries.

## Pipeline Stages

1. **Scrape** — collect articles from RSS feeds and websites
2. **Clean** — extract text from HTML, deduplicate, filter by language
3. **Bias classify** — label each article's political leaning (left/center/right)
4. **Embed** — generate vector representations for semantic similarity
5. **Cluster** — group articles covering the same story into events
6. **Summarize + tag** — LLM-generated neutral summary and topic per event
7. **Load** — persist everything to Postgres + pgvector

## Architecture

**Apache Airflow** orchestrates the pipeline. Each stage runs in its own
**Docker container** with its own dependencies, launched by Airflow via
`DockerOperator`.

Airflow does not run any stage logic itself — it launches containers, passes
object storage paths between them, and handles scheduling/retries.

### Why containers per stage

- **No dependency conflicts** — each container carries only what it needs
  (torch for bias, sentence-transformers for embeddings, hdbscan for
  clustering, etc.)
- **Swap any stage independently** — build a new image, update an Airflow
  Variable, next run uses it. No DAG changes needed.
- **Instant rollback** — old images stay in the registry, revert the
  Variable to the previous tag.
- **Experiment freely** — iterate locally, containerize only when it beats
  the baseline.

### How stages communicate

Every container reads NDJSON from object storage, writes NDJSON back, and
prints the output path to stdout (captured by Airflow via XCom). The NDJSON
field names are the contract — as long as a new container writes the same
fields, it's a valid swap.

### Data stores

- **Object storage** (S3/GCS/Azure Blob) — intermediate NDJSON between stages
- **Postgres + pgvector** — final destination for articles, events, embeddings
