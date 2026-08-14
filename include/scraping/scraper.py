"""
Scraping stage.

Responsible only for: fetch raw content, attach minimal metadata (source, url,
scraped_at), and land it in object storage as newline-delimited JSON. No
cleaning/parsing of article body happens here — that's preprocessing.py's job.
Keeping raw HTML around (not just extracted text) lets you re-run
preprocessing later without re-scraping if your extraction logic changes.
"""
from __future__ import annotations

import concurrent.futures
import datetime as dt
import hashlib
import json
import logging
from dataclasses import asdict, dataclass

import feedparser
import requests
import yaml

from include.config import CONFIG

logger = logging.getLogger(__name__)


@dataclass
class RawArticle:
    article_id: str          # stable hash of URL, used for dedup/idempotency downstream
    source: str
    url: str
    html: str
    scraped_at: str


def _article_id(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:24]


def load_sources() -> list[dict]:
    with open(CONFIG.sources_config_path) as f:
        return yaml.safe_load(f)


def _fetch_url(url: str, timeout: int) -> str | None:
    try:
        resp = requests.get(
            url,
            timeout=timeout,
            headers={"User-Agent": "news-pipeline-bot/1.0 (+contact: you@example.com)"},
        )
        resp.raise_for_status()
        return resp.text
    except requests.RequestException as e:
        logger.warning("Fetch failed for %s: %s", url, e)
        return None


def _discover_urls_rss(source: dict) -> list[str]:
    feed = feedparser.parse(source["url"])
    return [entry.link for entry in feed.entries]


def _discover_urls_html(source: dict) -> list[str]:
    # Minimal seed-page link discovery. For real sites you likely want scrapy/playwright
    # for JS-rendered pages — swap this function's implementation, the interface stays the same.
    urls: list[str] = []
    for seed in source.get("seed_urls", []):
        html = _fetch_url(seed, CONFIG.scrape_timeout_s)
        if not html:
            continue
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")
        for a in soup.select(source["article_link_selector"]):
            href = a.get("href")
            if href:
                urls.append(href)
    return urls


def discover_urls(source: dict) -> list[str]:
    if source["type"] == "rss":
        return _discover_urls_rss(source)
    if source["type"] == "html":
        return _discover_urls_html(source)
    raise ValueError(f"Unknown source type: {source['type']}")


def scrape_source(source: dict) -> list[RawArticle]:
    urls = discover_urls(source)
    articles: list[RawArticle] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=CONFIG.scrape_concurrency) as pool:
        future_to_url = {
            pool.submit(_fetch_url, url, CONFIG.scrape_timeout_s): url for url in urls
        }
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            html = future.result()
            if html is None:
                continue
            articles.append(
                RawArticle(
                    article_id=_article_id(url),
                    source=source["name"],
                    url=url,
                    html=html,
                    scraped_at=dt.datetime.utcnow().isoformat(),
                )
            )
    logger.info("Scraped %d/%d articles from %s", len(articles), len(urls), source["name"])
    return articles


def scrape_all(run_date: str) -> str:
    """
    Entry point called by the Airflow task. Scrapes every configured source and
    writes the results as NDJSON to object storage, returns the object key
    (Airflow XCom-friendly — small string, not the data itself).
    """
    from include.db.postgres_client import get_object_storage_hook  # local import: keep DAG parse fast

    sources = load_sources()
    all_articles: list[RawArticle] = []
    for source in sources:
        all_articles.extend(scrape_source(source))

    ndjson = "\n".join(json.dumps(asdict(a)) for a in all_articles)
    key = f"{CONFIG.raw_html_prefix.format(run_date=run_date)}articles.ndjson"

    hook = get_object_storage_hook()
    hook.write_text(key, ndjson)

    logger.info("Wrote %d raw articles to %s", len(all_articles), key)
    return key
