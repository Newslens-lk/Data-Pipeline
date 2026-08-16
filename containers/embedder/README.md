# Embedder Container

Reads cleaned articles from object storage, generates embeddings using `intfloat/multilingual-e5-large`, and writes `{article_id, embedding}` NDJSON back to object storage. Embeddings are consumed by both the bias classifier (XGBoost) and clustering stages.

## What it does

1. **Reads** cleaned articles NDJSON from MinIO
2. **Prepends** the E5 prefix (`"passage: "`) and concatenates title + body
3. **Tokenizes** with truncation at 512 tokens
4. **Embeds** using mean pooling over token embeddings (masked), then L2 normalizes
5. **Writes** `{article_id, embedding}` NDJSON back to MinIO

## I/O Contract

**Input** (from cleaner): `cleaned/{RUN_DATE}/articles.ndjson`
```json
{"article_id": "...", "source": "...", "url": "...", "title": "...", "body": "...", "language": "si", "published_at": "...", "scraped_at": "..."}
```

**Output** (for bias classifier + clustering): `embeddings/{RUN_DATE}/embeddings.ndjson`
```json
{"article_id": "...", "embedding": [0.024, -0.010, ...]}
```

Embedding dimension: 1024 (multilingual-e5-large).

## Two inference modes

| Mode | When | How |
|------|------|-----|
| **Modal (remote GPU)** | Local dev (no GPU) | Set `USE_MODAL=true`, mount `~/.modal.toml` into container |
| **Local GPU** | Production | Default (`USE_MODAL=false`), run with `--gpus all` and CUDA base image |

### Modal setup

1. `pip install modal` and `modal setup` (authenticates, saves token to `~/.modal.toml`)
2. `modal deploy modal_embedder.py` (deploys the remote GPU function)
3. Run container with `-e USE_MODAL=true -v ~/.modal.toml:/root/.modal.toml:ro`

## Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `INPUT_KEY` | Yes | S3 key of cleaned articles (e.g. `cleaned/2026-08-15/articles.ndjson`) |
| `STORAGE_ENDPOINT` | Yes | S3/MinIO endpoint |
| `STORAGE_BUCKET` | Yes | Bucket name |
| `AWS_ACCESS_KEY_ID` | Yes | S3/MinIO access key |
| `AWS_SECRET_ACCESS_KEY` | Yes | S3/MinIO secret key |
| `USE_MODAL` | No | `true` for remote GPU via Modal (default: `false`) |
| `EMBEDDING_MODEL` | No | HuggingFace model ID (default: `intfloat/multilingual-e5-large`) |
| `BATCH_SIZE` | No | Inference batch size (default: 32) |

## Build and run

```bash
# Build
docker build -t newslens/embedder:v1 .

# Run (local dev — Modal remote GPU)
docker run --rm --network newslens_pipeline_default --env-file ../../.env -e INPUT_KEY=cleaned/2026-08-15/articles.ndjson -e USE_MODAL=true -v ~/.modal.toml:/root/.modal.toml:ro newslens/embedder:v1

# Run (production — local GPU)
docker run --rm --gpus all --network newslens_pipeline_default --env-file ../../.env -e INPUT_KEY=cleaned/2026-08-15/articles.ndjson newslens/embedder:v1
```

## Why embeddings are a separate stage

The bias classifier (E5 + XGBoost) and clustering stage both need embeddings from the same model. Running embedding generation as its own stage means:
- The model runs **once**, not twice
- The bias classifier becomes a lightweight XGBoost-only container (~200MB instead of ~3GB)
- Swapping the embedding model happens in **one place**

The trade-off: the XGBoost classifier must be retrained if the embedding model changes, since it was trained on a specific model's representations.
