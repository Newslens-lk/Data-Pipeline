"""
Cleaner container entry point.

Reads raw NDJSON from object storage (scraper output), applies text cleaning
and normalization, deduplicates, filters, and writes cleaned NDJSON back to
object storage ready for the bias classifier.

Environment variables:
    INPUT_KEY             - S3 key of the scraper output NDJSON
    STORAGE_ENDPOINT      - S3/MinIO endpoint URL
    STORAGE_BUCKET        - bucket name
    AWS_ACCESS_KEY_ID     - S3/MinIO access key
    AWS_SECRET_ACCESS_KEY - S3/MinIO secret key
    MIN_BODY_CHARS        - minimum body length to keep (default: 50)
"""
from __future__ import annotations

import json
import logging
import os
import re
import unicodedata

import boto3

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

INPUT_KEY = os.environ["INPUT_KEY"]
STORAGE_ENDPOINT = os.environ["STORAGE_ENDPOINT"]
STORAGE_BUCKET = os.environ["STORAGE_BUCKET"]
MIN_BODY_CHARS = int(os.environ.get("MIN_BODY_CHARS", "50"))

# Zero-width and invisible characters common in web-scraped Sinhala text
_INVISIBLE_RE = re.compile(
    "[\u200b\u200c\u200d\u200e\u200f\u2060\u2061\u2062\u2063\u2064\ufeff]"
)
_WHITESPACE_RE = re.compile(r"\s+")
_HTML_ENTITIES = {
    "&nbsp;": " ",
    "&amp;": "&",
    "&lt;": "<",
    "&gt;": ">",
    "&quot;": '"',
    "&#39;": "'",
    "&apos;": "'",
}


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=STORAGE_ENDPOINT,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )


def normalize_text(text: str) -> str:
    """Unicode NFC normalize, strip invisible chars, clean whitespace."""
    # NFC normalization — standardizes Sinhala character representations
    text = unicodedata.normalize("NFC", text)

    # Remove zero-width and invisible characters
    text = _INVISIBLE_RE.sub("", text)

    # Replace leftover HTML entities
    for entity, replacement in _HTML_ENTITIES.items():
        text = text.replace(entity, replacement)

    # Collapse whitespace
    text = _WHITESPACE_RE.sub(" ", text).strip()

    return text


def deduplicate(articles: list[dict]) -> list[dict]:
    """Remove exact duplicates (same article_id) and near-duplicates (same first 300 chars)."""
    seen_ids: set[str] = set()
    seen_shingles: set[str] = set()
    unique = []

    for a in articles:
        # Exact duplicate by article_id
        if a["article_id"] in seen_ids:
            continue
        seen_ids.add(a["article_id"])

        # Near-duplicate by first 300 chars of body
        shingle = re.sub(r"\W+", "", a["body"][:300].lower())
        if shingle in seen_shingles:
            continue
        seen_shingles.add(shingle)

        unique.append(a)

    return unique


def clean_articles(raw_ndjson: str) -> list[dict]:
    articles = []

    for line in raw_ndjson.strip().splitlines():
        article = json.loads(line)

        # Normalize title and body
        article["title"] = normalize_text(article["title"])
        article["body"] = normalize_text(article["body"])

        # Filter: too short
        if len(article["body"]) < MIN_BODY_CHARS:
            continue

        articles.append(article)

    # Deduplicate
    before = len(articles)
    articles = deduplicate(articles)
    if before != len(articles):
        logger.info("Dedup removed %d articles", before - len(articles))

    return articles


def main():
    s3 = get_s3_client()

    # Read scraper output
    resp = s3.get_object(Bucket=STORAGE_BUCKET, Key=INPUT_KEY)
    raw_ndjson = resp["Body"].read().decode("utf-8")
    logger.info("Read %s (%d bytes)", INPUT_KEY, len(raw_ndjson))

    articles = clean_articles(raw_ndjson)

    if not articles:
        logger.warning("No articles after cleaning. Exiting.")
        print("")
        return

    # Write cleaned output
    out_key = INPUT_KEY.replace("raw/", "cleaned/")
    ndjson = "\n".join(json.dumps(a, ensure_ascii=False) for a in articles)
    s3.put_object(Bucket=STORAGE_BUCKET, Key=out_key, Body=ndjson.encode("utf-8"))

    logger.info("Wrote %d cleaned articles to s3://%s/%s", len(articles), STORAGE_BUCKET, out_key)
    print(out_key)


if __name__ == "__main__":
    main()
