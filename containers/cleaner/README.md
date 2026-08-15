# Cleaner Container

Reads raw scraped articles from object storage, cleans and normalizes the text, deduplicates, and writes the cleaned NDJSON back to object storage ready for the bias classifier.

## What it does

1. **Unicode normalization (NFC)** — standardizes Sinhala character representations so the same character isn't stored multiple ways
2. **Zero-width character removal** — strips invisible characters (zero-width spaces, joiners, etc.) that creep into web-scraped text
3. **HTML entity cleanup** — converts leftover `&nbsp;`, `&amp;`, etc. to their actual characters
4. **Whitespace normalization** — collapses multiple spaces/newlines into single spaces, trims edges
5. **Length filtering** — drops articles with body shorter than `MIN_BODY_CHARS`
6. **Deduplication** — removes exact duplicates (same `article_id`) and near-duplicates (same first 300 chars)

Both **title** and **body** go through the full normalization pipeline.

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

## Why this stage exists

Web-scraped text is messy. Even after the scraper extracts the article body, the raw text contains invisible characters, inconsistent whitespace, and encoding artifacts that will cause problems downstream for ML models (bias classifier, embedder).

### The Sinhala zero-width joiner problem

Sinhala text on the web is full of zero-width joiners (`\u200d`). These are invisible Unicode characters that browsers use to control how letters connect visually. For example, `ශ්‍රී` (with a hidden joiner) and `ශ්රී` (without) look identical but are different byte sequences. If you don't normalize these, the same word gets treated as two different tokens by ML models, and duplicate detection fails on articles that are actually identical.

In our test run, **1,928 zero-width characters** were found across 36 articles — affecting 33 of them.

### What each cleaning step does

**Unicode NFC normalization**: Unicode allows the same character to be represented in multiple ways (composed vs decomposed forms). NFC (Normal Form Composed) picks one canonical representation. This is important for Sinhala because vowel signs and consonant clusters can be encoded differently by different websites.

**Zero-width character removal**: Strips `\u200b` (zero-width space), `\u200c` (zero-width non-joiner), `\u200d` (zero-width joiner), `\u200e`/`\u200f` (directional marks), `\u2060` (word joiner), and `\ufeff` (byte order mark). These are all invisible in rendered text but affect string comparison, tokenization, and model input.

**HTML entity cleanup**: Sometimes the scraper's HTML-to-text extraction leaves behind encoded entities like `&nbsp;` (non-breaking space) or `&amp;`. These get converted to their actual characters.

**Whitespace normalization**: Collapses runs of spaces, tabs, and newlines into single spaces and trims the edges. Web-scraped text often has double newlines between paragraphs, trailing spaces, or tab characters from HTML formatting.

**Length filtering**: Articles with very short bodies (< 50 chars by default) are dropped — these are usually navigation text, error pages, or articles that failed to parse properly.

**Deduplication**: Two passes:
- *Exact dedup*: if two articles have the same `article_id` (SHA-256 of URL), keep only the first. This catches the same URL appearing twice in a scrape.
- *Near-dedup*: if the first 300 characters of the body (lowercased, non-word chars stripped) match another article, it's a near-duplicate. This catches the same article published under different URLs.
