"""
Bias classifier container (Transformers/HelaBERT) entry point.

Reads cleaned articles NDJSON from MinIO, classifies bias using a fine-tuned
HelaBERT model, and writes bias results NDJSON back to MinIO.
Prints the output key to stdout for Airflow to capture via XCom.

Supports two modes:
    - USE_MODAL=true  → sends texts to Modal for remote GPU inference (local dev)
    - USE_MODAL=false → runs inference locally (production with GPU)

Environment variables:
    INPUT_KEY             - MinIO key for cleaned articles NDJSON
    STORAGE_ENDPOINT      - S3/MinIO endpoint URL
    STORAGE_BUCKET        - bucket name
    AWS_ACCESS_KEY_ID     - S3/MinIO access key
    AWS_SECRET_ACCESS_KEY - S3/MinIO secret key
    MODEL_URI             - HuggingFace model path or S3 key (default: models/bias-classifier-helabert)
    BATCH_SIZE            - inference batch size (default: 32)
    USE_MODAL             - "true" to use Modal remote GPU (default: "false")
"""
from __future__ import annotations

import json
import logging
import os

import boto3

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

INPUT_KEY = os.environ["INPUT_KEY"]
STORAGE_ENDPOINT = os.environ["STORAGE_ENDPOINT"]
STORAGE_BUCKET = os.environ["STORAGE_BUCKET"]
MODEL_URI = os.environ.get("MODEL_URI", "models/bias-classifier-helabert")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "32"))
USE_MODAL = os.environ.get("USE_MODAL", "false").lower() == "true"

LABELS = ["far_left", "left", "center", "right", "far_right"]


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=STORAGE_ENDPOINT,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )


# --- Local GPU inference ---

def load_model():
    import torch
    from transformers import AutoModelForSequenceClassification, AutoTokenizer

    logger.info("Loading HelaBERT model locally: %s", MODEL_URI)
    tokenizer = AutoTokenizer.from_pretrained(MODEL_URI)
    model = AutoModelForSequenceClassification.from_pretrained(MODEL_URI)
    model.eval()
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model.to(device)
    logger.info("Model loaded on %s", device)
    return tokenizer, model, device


def classify_batch_local(texts: list[str], tokenizer, model, device) -> list[list[float]]:
    import torch

    inputs = tokenizer(
        texts, padding=True, truncation=True, max_length=512, return_tensors="pt"
    ).to(device)

    with torch.no_grad():
        logits = model(**inputs).logits
        probs = torch.softmax(logits, dim=-1).cpu().tolist()

    return probs


# --- Modal remote GPU inference ---

def classify_batch_modal(texts: list[str]) -> list[list[float]]:
    import modal

    BiasClassifier = modal.Cls.from_name("newslens-bias-classifier", "BiasClassifier")
    classifier = BiasClassifier()
    return classifier.classify.remote(texts)


# --- Main ---

def main():
    s3 = get_s3_client()

    # Read cleaned articles
    obj = s3.get_object(Bucket=STORAGE_BUCKET, Key=INPUT_KEY)
    lines = obj["Body"].read().decode("utf-8").strip().splitlines()
    articles = [json.loads(line) for line in lines]
    logger.info("Read %d articles from s3://%s/%s", len(articles), STORAGE_BUCKET, INPUT_KEY)

    # Prepare texts
    texts = [f"{a['title']}. {a['body']}" for a in articles]

    # Load local model if not using Modal
    if USE_MODAL:
        logger.info("Using Modal remote GPU for inference")
    else:
        tokenizer, model, device = load_model()

    # Classify in batches
    results = []
    for i in range(0, len(texts), BATCH_SIZE):
        batch = texts[i : i + BATCH_SIZE]

        if USE_MODAL:
            batch_probs = classify_batch_modal(batch)
        else:
            batch_probs = classify_batch_local(batch, tokenizer, model, device)

        for j, probs in enumerate(batch_probs):
            scores = {label: float(probs[k]) for k, label in enumerate(LABELS)}
            top_label = max(scores, key=scores.get)
            results.append({
                "article_id": articles[i + j]["article_id"],
                "bias_label": top_label,
                "bias_confidence": scores[top_label],
                "bias_scores": scores,
            })
        logger.info("Classified batch %d/%d", i // BATCH_SIZE + 1, (len(texts) - 1) // BATCH_SIZE + 1)

    # Write output
    ndjson = "\n".join(json.dumps(r) for r in results)
    date_part = INPUT_KEY.split("/")[1]
    out_key = f"bias/{date_part}/bias_results.ndjson"
    s3.put_object(Bucket=STORAGE_BUCKET, Key=out_key, Body=ndjson.encode("utf-8"))

    logger.info("Classified %d articles -> s3://%s/%s", len(results), STORAGE_BUCKET, out_key)
    print(out_key)


if __name__ == "__main__":
    main()
