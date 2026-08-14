"""
Summarization stage — for each event cluster, prompts an LLM for a summary
that synthesizes across the articles in that cluster (not per-article).
"""
from __future__ import annotations

import json
import logging
from collections import defaultdict
from dataclasses import asdict, dataclass

import anthropic

from include.config import CONFIG

logger = logging.getLogger(__name__)

_SUMMARY_PROMPT = """You are summarizing a cluster of news articles that all cover the same event.

Below are excerpts from {n} articles about the same event, from different sources.
Write a neutral, factual summary of the event in 3-5 sentences. Do not favor any
single source's framing. Note if sources disagree on key facts.

Articles:
{articles_block}

Respond with ONLY the summary text, no preamble."""


@dataclass
class EventSummary:
    event_id: str
    summary: str
    article_count: int
    source_count: int


def _client() -> anthropic.Anthropic:
    return anthropic.Anthropic()  # picks up ANTHROPIC_API_KEY from env / Airflow connection


def summarize_event(event_id: str, articles: list[dict]) -> EventSummary:
    articles_block = "\n\n".join(
        f"[Source: {a['source']}]\n{a['title']}\n{a['body'][:1500]}" for a in articles[:10]
        # cap at 10 articles / 1500 chars each to keep prompt bounded for large events
    )
    prompt = _SUMMARY_PROMPT.format(n=len(articles), articles_block=articles_block)

    resp = _client().messages.create(
        model=CONFIG.llm_model,
        max_tokens=CONFIG.llm_max_tokens,
        messages=[{"role": "user", "content": prompt}],
    )
    summary_text = "".join(block.text for block in resp.content if block.type == "text").strip()

    return EventSummary(
        event_id=event_id,
        summary=summary_text,
        article_count=len(articles),
        source_count=len({a["source"] for a in articles}),
    )


def summarize_events(clean_key: str, clusters_key: str) -> str:
    """Entry point called by the Airflow task."""
    from include.db.postgres_client import get_object_storage_hook

    hook = get_object_storage_hook()
    articles = {a["article_id"]: a for a in (json.loads(l) for l in hook.read_text(clean_key).splitlines())}
    assignments = [json.loads(l) for l in hook.read_text(clusters_key).splitlines()]

    articles_by_event: dict[str, list[dict]] = defaultdict(list)
    for a in assignments:
        if a["event_id"]:
            articles_by_event[a["event_id"]].append(articles[a["article_id"]])

    summaries = [
        summarize_event(event_id, event_articles)
        for event_id, event_articles in articles_by_event.items()
    ]

    out_key = clusters_key.replace("clusters.ndjson", "event_summaries.ndjson")
    ndjson = "\n".join(json.dumps(asdict(s)) for s in summaries)
    hook.write_text(out_key, ndjson)

    logger.info("Summarized %d events -> %s", len(summaries), out_key)
    return out_key
