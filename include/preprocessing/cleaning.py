"""
Cleaning & preprocessing stage.

Input: raw NDJSON (RawArticle records with full HTML) from object storage.
Output: NDJSON of CleanArticle records (extracted title/body/date, language,
dedup flag) written back to object storage, ready for the ML stages.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import re
from dataclasses import asdict, dataclass

from bs4 import BeautifulSoup

from include.config import CONFIG

logger = logging.getLogger(__name__)

_WHITESPACE_RE = re.compile(r"\s+")


@dataclass
class CleanArticle:
    article_id: str
    source: str
    url: str
    title: str
    body: str
    published_at: str | None
    language: str
    scraped_at: str
    is_duplicate: bool = False


def extract_text(html: str) -> tuple[str, str]:
    """Very generic extraction. Swap in `trafilatura` or `newspaper3k` for better
    boilerplate removal if generic selectors aren't good enough for your sources."""
    soup = BeautifulSoup(html, "html.parser")

    title_tag = soup.find("h1") or soup.find("title")
    title = title_tag.get_text(strip=True) if title_tag else ""

    # crude body heuristic: largest concentration of <p> text
    paragraphs = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
    body = " ".join(paragraphs)

    return title, body


def normalize_whitespace(text: str) -> str:
    return _WHITESPACE_RE.sub(" ", text).strip()


def detect_language(text: str) -> str:
    from langdetect import LangDetectException, detect  # lazy: keeps module import light for unit tests

    try:
        return detect(text[:1000])
    except LangDetectException:
        return "unknown"


def near_duplicate_ids(articles: list[CleanArticle]) -> set[str]:
    """
    Placeholder near-dup detection via a cheap shingling/minhash approach.
    For production, prefer comparing embeddings (cosine > threshold) once the
    embedding stage has run — this cheap pass just catches obvious exact/near
    text duplicates before spending compute on embeddings.
    """
    seen_hashes: dict[str, str] = {}
    duplicates: set[str] = set()
    for a in articles:
        shingle = a.body[:300].lower()
        key = re.sub(r"\W+", "", shingle)
        if key in seen_hashes:
            duplicates.add(a.article_id)
        else:
            seen_hashes[key] = a.article_id
    return duplicates


def clean_raw_articles(raw_key: str) -> str:
    """Entry point called by the Airflow task."""
    from include.db.postgres_client import get_object_storage_hook

    hook = get_object_storage_hook()
    raw_ndjson = hook.read_text(raw_key)

    cleaned: list[CleanArticle] = []
    for line in raw_ndjson.splitlines():
        raw = json.loads(line)
        title, body = extract_text(raw["html"])
        body = normalize_whitespace(body)
        title = normalize_whitespace(title)

        if len(body) < CONFIG.min_article_chars:
            continue

        language = detect_language(body)
        if language not in CONFIG.allowed_languages:
            continue

        cleaned.append(
            CleanArticle(
                article_id=raw["article_id"],
                source=raw["source"],
                url=raw["url"],
                title=title,
                body=body,
                published_at=None,  # TODO: parse from date_selector / RSS entry if available
                language=language,
                scraped_at=raw["scraped_at"],
            )
        )

    dup_ids = near_duplicate_ids(cleaned)
    for a in cleaned:
        a.is_duplicate = a.article_id in dup_ids
    cleaned = [a for a in cleaned if not a.is_duplicate]

    out_key = raw_key.replace("raw/", "clean/").replace("articles.ndjson", "clean_articles.ndjson")
    ndjson = "\n".join(json.dumps(asdict(a)) for a in cleaned)
    hook.write_text(out_key, ndjson)

    logger.info("Cleaned %d articles -> %s", len(cleaned), out_key)
    return out_key
