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
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
import os

import boto3

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

RUN_DATE = os.environ.get("RUN_DATE", dt.date.today().isoformat())
STORAGE_ENDPOINT = os.environ["STORAGE_ENDPOINT"]
STORAGE_BUCKET = os.environ["STORAGE_BUCKET"]

# Sinhala sources only
SINHALA_SOURCES = [
    "AdaDeranaSinhalaLk",
    "AdaLk",
    "DivainaLk",
    "LankadeepaLk",
]


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=STORAGE_ENDPOINT,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )


def article_id(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:24]


def scrape_articles() -> list[dict]:
    from news_lk3.custom_newspapers import (
        AdaDeranaSinhalaLk,
        AdaLk,
        DivainaLk,
        LankadeepaLk,
    )

    source_classes = {
        "AdaDeranaSinhalaLk": AdaDeranaSinhalaLk,
        "AdaLk": AdaLk,
        "DivainaLk": DivainaLk,
        "LankadeepaLk": LankadeepaLk,
    }

    articles = []
    scraped_at = dt.datetime.utcnow().isoformat()

    for name in SINHALA_SOURCES:
        cls = source_classes[name]
        logger.info("Scraping %s ...", name)
        try:
            url_metadata_set = set()  # don't skip any URLs
            count = 0
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
                count += 1
            logger.info("Got %d articles from %s", count, name)
        except Exception:
            logger.exception("Failed to scrape %s", name)

    return articles


def main():
    articles = scrape_articles()

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
