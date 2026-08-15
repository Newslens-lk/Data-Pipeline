# Cleaner Container

Reads raw scraped articles from object storage, cleans and normalizes the text, deduplicates, filters by language, and writes the cleaned NDJSON back to object storage.

## What it does

1. **Unicode normalization (NFC)** — standardizes Sinhala character representations so the same character isn't stored multiple ways
2. **Zero-width character removal** — strips invisible characters (zero-width spaces, joiners, etc.) that creep into web-scraped text
3. **HTML entity cleanup** — converts leftover `&nbsp;`, `&amp;`, etc. to their actual characters
4. **Whitespace normalization** — collapses multiple spaces/newlines into single spaces, trims edges
5. **Language filtering** — keeps only Sinhala (`si`) articles using `langdetect`
6. **Length filtering** — drops articles with body shorter than `MIN_BODY_CHARS`
7. **Deduplication** — removes exact duplicates (same `article_id`) and near-duplicates (same first 300 chars)

## I/O Contract

**Input** (from scraper): `raw/{RUN_DATE}/articles.ndjson`
```json
{"article_id": "...", "source": "...", "url": "...", "title": "...", "body": "...", "language": "si", "published_at": "...", "scraped_at": "..."}
```

**Output** (for bias classifier): `cleaned/{RUN_DATE}/articles.ndjson`

Same fields, but with cleaned/normalized text and duplicates removed.

## Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `INPUT_KEY` | Yes | S3 key of scraper output (e.g. `raw/2026-08-16/articles.ndjson`) |
| `STORAGE_ENDPOINT` | Yes | S3/MinIO endpoint |
| `STORAGE_BUCKET` | Yes | Bucket name |
| `AWS_ACCESS_KEY_ID` | Yes | S3/MinIO access key |
| `AWS_SECRET_ACCESS_KEY` | Yes | S3/MinIO secret key |
| `MIN_BODY_CHARS` | No | Minimum body length to keep (default: 50) |

## Build and run

```bash
docker build -t newslens/cleaner:v1 .
docker run --rm --network newslens_pipeline_default --env-file ../../.env -e INPUT_KEY=raw/2026-08-16/articles.ndjson newslens/cleaner:v1
```
