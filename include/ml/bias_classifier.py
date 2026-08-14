"""
Bias classification stage — runs your custom fine-tuned model over cleaned
articles.

Model loading is intentionally lazy + module-level cached: Airflow may run
this task in a fresh worker process, but within one task execution we don't
want to reload the model per-article. Batch everything.
"""
from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from functools import lru_cache

from include.config import CONFIG

logger = logging.getLogger(__name__)


@dataclass
class BiasResult:
    article_id: str
    bias_label: str
    bias_confidence: float
    bias_scores: dict  # full per-class probability distribution, useful for audits/thresholding later


@lru_cache(maxsize=1)
def _load_model():
    """
    Loads the fine-tuned classifier once per worker process.

    Swap the body of this function for however you actually load your model —
    e.g. a HuggingFace `AutoModelForSequenceClassification` checkpoint pulled
    from CONFIG.bias_model_uri (S3/GCS path or HF hub repo), or an MLflow
    `pyfunc` model. Keeping this behind one function means the rest of the
    module (and the Airflow task) doesn't care which framework you used.
    """
    import torch
    from transformers import AutoModelForSequenceClassification, AutoTokenizer

    logger.info("Loading bias classifier from %s", CONFIG.bias_model_uri)
    tokenizer = AutoTokenizer.from_pretrained(CONFIG.bias_model_uri)
    model = AutoModelForSequenceClassification.from_pretrained(CONFIG.bias_model_uri)
    model.eval()
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model.to(device)
    return tokenizer, model, device


def classify_batch(texts: list[str]) -> list[tuple[str, float, dict]]:
    import torch

    tokenizer, model, device = _load_model()
    results = []

    for i in range(0, len(texts), CONFIG.bias_batch_size):
        batch = texts[i : i + CONFIG.bias_batch_size]
        inputs = tokenizer(
            batch, padding=True, truncation=True, max_length=512, return_tensors="pt"
        ).to(device)

        with torch.no_grad():
            logits = model(**inputs).logits
            probs = torch.softmax(logits, dim=-1).cpu().numpy()

        for p in probs:
            scores = {label: float(p[i]) for i, label in enumerate(CONFIG.bias_labels)}
            top_label = max(scores, key=scores.get)
            results.append((top_label, scores[top_label], scores))

    return results


def classify_articles(clean_key: str) -> str:
    """Entry point called by the Airflow task."""
    from include.db.postgres_client import get_object_storage_hook

    hook = get_object_storage_hook()
    articles = [json.loads(line) for line in hook.read_text(clean_key).splitlines()]

    texts = [a["title"] + ". " + a["body"] for a in articles]
    predictions = classify_batch(texts)

    results = [
        BiasResult(article_id=a["article_id"], bias_label=label, bias_confidence=conf, bias_scores=scores)
        for a, (label, conf, scores) in zip(articles, predictions)
    ]

    out_key = clean_key.replace("clean_articles.ndjson", "bias_results.ndjson")
    ndjson = "\n".join(json.dumps(asdict(r)) for r in results)
    hook.write_text(out_key, ndjson)

    logger.info("Classified bias for %d articles -> %s", len(results), out_key)
    return out_key
