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
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

RUN_DATE = os.environ.get("RUN_DATE", dt.date.today().isoformat())
STORAGE_ENDPOINT = os.environ["STORAGE_ENDPOINT"]
STORAGE_BUCKET = os.environ["STORAGE_BUCKET"]
SCRAPE_TIMEOUT = int(os.environ.get("SCRAPE_TIMEOUT", "300"))
HIRU_MAX_PAGES = int(os.environ.get("HIRU_MAX_PAGES", "5"))

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


HIRU_API_URL = "https://hirunews.lk/api/fetch_news.php"
HIRU_BASE_URL = "https://hirunews.lk"


def scrape_hiru() -> list[dict]:
    """Scrape Hiru News via their JSON API (no HTML parsing needed)."""
    articles = []
    scraped_at = dt.datetime.utcnow().isoformat()
    seen_ids = set()

    signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(SCRAPE_TIMEOUT)

    try:
        for page in range(1, HIRU_MAX_PAGES + 1):
            logger.info("Hiru News: fetching API page %d/%d", page, HIRU_MAX_PAGES)

            resp = requests.get(
                HIRU_API_URL,
                params={"page": page, "category": "General"},
                timeout=30,
            )
            if resp.status_code != 200:
                logger.warning("Hiru News: API page %d returned %d, skipping", page, resp.status_code)
                continue

            items = resp.json()
            if not items:
                logger.info("Hiru News: no more articles at page %d, stopping", page)
                break

            for item in items:
                art_id = item.get("sinhala_art_id", "")
                if not art_id or art_id in seen_ids:
                    continue
                seen_ids.add(art_id)

                title = item.get("sinhala_title", "").strip()
                body = item.get("sinhala_story", "").strip()
                if len(body) < 50:
                    continue

                slug = item.get("seourltitle", "")
                url = f"{HIRU_BASE_URL}/{slug}" if slug else ""

                published_at = None
                ts_text = item.get("sinhala_added_date", "")
                if ts_text:
                    try:
                        published_at = dt.datetime.strptime(
                            ts_text, "%Y-%m-%d %H:%M:%S"
                        ).isoformat()
                    except ValueError:
                        pass

                articles.append({
                    "article_id": article_id(url) if url else art_id,
                    "source": "hirunews",
                    "url": url,
                    "title": title,
                    "body": body,
                    "language": "si",
                    "published_at": published_at,
                    "scraped_at": scraped_at,
                })

        logger.info("Got %d articles from Hiru News", len(articles))
    except SourceTimeout:
        logger.warning("Timeout after %ds scraping Hiru News (got %d articles so far)",
                       SCRAPE_TIMEOUT, len(articles))
    except Exception:
        logger.exception("Failed to scrape Hiru News")
    finally:
        signal.alarm(0)

    return articles


BBC_SINHALA_RSS = "https://feeds.bbci.co.uk/sinhala/rss.xml"
BBC_USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"


def _fetch_bbc_article_body(url: str) -> str:
    """Fetch full article body from a BBC Sinhala article page.

    BBC embeds content in <p> tags. We extract all paragraphs containing
    Sinhala Unicode characters (U+0D80–U+0DFF) and skip navigation/boilerplate.
    """
    import re

    resp = requests.get(url, headers={"User-Agent": BBC_USER_AGENT}, timeout=30)
    if resp.status_code != 200:
        return ""

    paragraphs = re.findall(r"<p[^>]*>(.*?)</p>", resp.text, re.DOTALL)
    sinhala_paras = []
    for p in paragraphs:
        clean = re.sub(r"<[^>]+>", "", p).strip()
        # Keep only paragraphs with Sinhala script content
        if re.search(r"[\u0D80-\u0DFF]", clean) and len(clean) > 20:
            sinhala_paras.append(clean)

    # Skip the first paragraph — it's typically the nav/header text
    if sinhala_paras and len(sinhala_paras[0]) > 200:
        sinhala_paras = sinhala_paras[1:]

    return "\n\n".join(sinhala_paras)


def scrape_bbc() -> list[dict]:
    """Scrape BBC Sinhala via RSS feed + full article fetch."""
    import re
    import xml.etree.ElementTree as ET
    from email.utils import parsedate_to_datetime

    articles = []
    scraped_at = dt.datetime.utcnow().isoformat()

    signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(SCRAPE_TIMEOUT)

    try:
        logger.info("BBC Sinhala: fetching RSS feed")
        resp = requests.get(BBC_SINHALA_RSS, timeout=30)
        if resp.status_code != 200:
            logger.warning("BBC Sinhala: RSS returned %d", resp.status_code)
            return articles

        root = ET.fromstring(resp.content)
        items = root.findall(".//item")
        logger.info("BBC Sinhala: found %d items in RSS", len(items))

        for item in items:
            link = item.findtext("link", "")
            # Strip RSS tracking params
            url = re.sub(r"\?at_medium=.*$", "", link)
            if not url:
                continue

            title = item.findtext("title", "").strip()

            # Parse RFC 2822 date from RSS
            published_at = None
            pub_date = item.findtext("pubDate", "")
            if pub_date:
                try:
                    published_at = parsedate_to_datetime(pub_date).isoformat()
                except Exception:
                    pass

            # Fetch full article body
            body = _fetch_bbc_article_body(url)
            if len(body) < 50:
                logger.warning("BBC Sinhala: skipping %s (body too short)", url)
                continue

            articles.append({
                "article_id": article_id(url),
                "source": "bbc_sinhala",
                "url": url,
                "title": title,
                "body": body,
                "language": "si",
                "published_at": published_at,
                "scraped_at": scraped_at,
            })

        logger.info("Got %d articles from BBC Sinhala", len(articles))
    except SourceTimeout:
        logger.warning("Timeout after %ds scraping BBC Sinhala (got %d articles so far)",
                       SCRAPE_TIMEOUT, len(articles))
    except Exception:
        logger.exception("Failed to scrape BBC Sinhala")
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

    # lk_news sources
    for name in SINHALA_SOURCES:
        cls = source_classes[name]
        logger.info("Scraping %s ...", name)
        all_articles.extend(scrape_source(cls, name))

    # Hiru News (custom scraper)
    logger.info("Scraping Hiru News ...")
    all_articles.extend(scrape_hiru())

    # BBC Sinhala (RSS + full article fetch)
    logger.info("Scraping BBC Sinhala ...")
    all_articles.extend(scrape_bbc())

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
