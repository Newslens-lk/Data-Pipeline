"""
Scraper container entry point.

Uses the lk_news package to scrape Sinhala news sources. Converts articles
to the pipeline's NDJSON contract and writes to object storage (S3/MinIO).
Prints the output key to stdout for Airflow to capture via XCom.

Environment variables:
    RUN_DATE          - date string for the output key prefix (e.g. "2026-08-15")
    STORAGE_ENDPOINT  - S3/MinIO endpoint URL (e.g. "http://minio:9000")
    STORAGE_BUCKET    - bucket name (e.g. "newslens-pipeline")
    AWS_ACCESS_KEY_ID - S3/MinIO access key
    AWS_SECRET_ACCESS_KEY - S3/MinIO secret key
    SCRAPE_TIMEOUT    - max seconds per source (default: 300)
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
import os
import signal

import boto3

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

RUN_DATE = os.environ.get("RUN_DATE", dt.date.today().isoformat())
STORAGE_ENDPOINT = os.environ["STORAGE_ENDPOINT"]
STORAGE_BUCKET = os.environ["STORAGE_BUCKET"]
SCRAPE_TIMEOUT = int(os.environ.get("SCRAPE_TIMEOUT", "300"))

# Sinhala sources — skip AdaDeranaSinhalaLk (requires selenium read_selenium which is broken)
SINHALA_SOURCES = [
    "DivainaLk",
    "LankadeepaLk",
]


class SourceTimeout(Exception):
    pass


def _timeout_handler(signum, frame):
    raise SourceTimeout()


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=STORAGE_ENDPOINT,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )


def article_id(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:24]


def scrape_source(cls, name: str) -> list[dict]:
    """Scrape a single source with a timeout."""
    articles = []
    scraped_at = dt.datetime.utcnow().isoformat()

    signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(SCRAPE_TIMEOUT)

    try:
        url_metadata_set = set()
        for article in cls.gen_articles(url_metadata_set):
            body = "\n\n".join(article.original_body_lines)
            if len(body.strip()) < 50:
                continue

            articles.append({
                "article_id": article_id(article.url),
                "source": article.newspaper_id,
                "url": article.url,
                "title": article.original_title,
                "body": body,
                "language": article.original_lang,
                "published_at": dt.datetime.utcfromtimestamp(
                    article.time_ut
                ).isoformat() if article.time_ut else None,
                "scraped_at": scraped_at,
            })
        logger.info("Got %d articles from %s", len(articles), name)
    except SourceTimeout:
        logger.warning("Timeout after %ds scraping %s (got %d articles so far)",
                       SCRAPE_TIMEOUT, name, len(articles))
    except Exception:
        logger.exception("Failed to scrape %s", name)
    finally:
        signal.alarm(0)

    return articles


def scrape_all() -> list[dict]:
    from news_lk3.custom_newspapers import (
        DivainaLk,
        LankadeepaLk,
    )

    source_classes = {
        "DivainaLk": DivainaLk,
        "LankadeepaLk": LankadeepaLk,
    }

    all_articles = []
    for name in SINHALA_SOURCES:
        cls = source_classes[name]
        logger.info("Scraping %s ...", name)
        all_articles.extend(scrape_source(cls, name))

    return all_articles


def main():
    articles = scrape_all()

    if not articles:
        logger.warning("No articles scraped. Exiting.")
        print("")
        return

    ndjson = "\n".join(json.dumps(a, ensure_ascii=False) for a in articles)
    key = f"raw/{RUN_DATE}/articles.ndjson"

    s3 = get_s3_client()
    s3.put_object(Bucket=STORAGE_BUCKET, Key=key, Body=ndjson.encode("utf-8"))

    logger.info("Wrote %d articles to s3://%s/%s", len(articles), STORAGE_BUCKET, key)
    print(key)


if __name__ == "__main__":
    main()
