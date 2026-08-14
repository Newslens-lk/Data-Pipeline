"""
Topic assignment stage — tags each event with a topic. Reuses the LLM call
from summarization where possible (combine into one prompt) to save latency
and cost; kept as a separate module/task here for clarity and so you can
swap in a cheaper classifier later without touching summarization.
"""
from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass

import anthropic
import yaml

from include.config import CONFIG

logger = logging.getLogger(__name__)

_TOPIC_PROMPT = """Assign exactly one topic to the following news event summary.
Choose the single best-fitting topic from this list: {taxonomy}

Event summary:
{summary}

Respond with ONLY the topic label from the list, nothing else."""


@dataclass
class EventTopic:
    event_id: str
    topic: str


def _client() -> anthropic.Anthropic:
    return anthropic.Anthropic()


def _load_taxonomy() -> list[str]:
    with open(CONFIG.topic_taxonomy_path) as f:
        return yaml.safe_load(f)


def assign_topic(event_id: str, summary: str, taxonomy: list[str]) -> EventTopic:
    prompt = _TOPIC_PROMPT.format(taxonomy=", ".join(taxonomy), summary=summary)
    resp = _client().messages.create(
        model=CONFIG.llm_model,
        max_tokens=20,
        messages=[{"role": "user", "content": prompt}],
    )
    topic = "".join(block.text for block in resp.content if block.type == "text").strip().lower()
    if topic not in taxonomy:
        logger.warning("Model returned out-of-taxonomy topic '%s' for event %s; defaulting to 'other'", topic, event_id)
        topic = "other"
    return EventTopic(event_id=event_id, topic=topic)


def assign_topics(summaries_key: str) -> str:
    """Entry point called by the Airflow task."""
    from include.db.postgres_client import get_object_storage_hook

    hook = get_object_storage_hook()
    summaries = [json.loads(l) for l in hook.read_text(summaries_key).splitlines()]
    taxonomy = _load_taxonomy()

    topics = [assign_topic(s["event_id"], s["summary"], taxonomy) for s in summaries]

    out_key = summaries_key.replace("event_summaries.ndjson", "event_topics.ndjson")
    ndjson = "\n".join(json.dumps(asdict(t)) for t in topics)
    hook.write_text(out_key, ndjson)

    logger.info("Assigned topics for %d events -> %s", len(topics), out_key)
    return out_key
