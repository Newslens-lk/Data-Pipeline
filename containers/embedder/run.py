"""
Embedder container entry point.

Reads cleaned articles NDJSON from MinIO, generates embeddings using
multilingual-e5-large, and writes {article_id, embedding} NDJSON back to MinIO.
Prints the output key to stdout for Airflow to capture via XCom.

Supports two modes:
    - USE_MODAL=true  → sends texts to Modal for remote GPU inference (local dev)
    - USE_MODAL=false → runs inference locally (production with GPU)

Environment variables:
    INPUT_KEY             - MinIO key for cleaned articles NDJSON
    STORAGE_ENDPOINT      - S3/MinIO endpoint URL (e.g. "http://minio:9000")
    STORAGE_BUCKET        - bucket name (e.g. "newslens-pipeline")
    AWS_ACCESS_KEY_ID     - S3/MinIO access key
    AWS_SECRET_ACCESS_KEY - S3/MinIO secret key
    EMBEDDING_MODEL       - HuggingFace model ID (default: intfloat/multilingual-e5-large)
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
EMBEDDING_MODEL = os.environ.get("EMBEDDING_MODEL", "intfloat/multilingual-e5-large")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "32"))
USE_MODAL = os.environ.get("USE_MODAL", "false").lower() == "true"


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
    from transformers import AutoModel, AutoTokenizer

    logger.info("Loading embedding model locally: %s", EMBEDDING_MODEL)
    tokenizer = AutoTokenizer.from_pretrained(EMBEDDING_MODEL)
    model = AutoModel.from_pretrained(EMBEDDING_MODEL)
    model.eval()
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model.to(device)
    logger.info("Model loaded on %s", device)
    return tokenizer, model, device


def embed_batch_local(texts: list[str], tokenizer, model, device) -> list[list[float]]:
    import torch
    import torch.nn.functional as F

    inputs = tokenizer(
        texts, padding=True, truncation=True, max_length=512, return_tensors="pt"
    ).to(device)

    with torch.no_grad():
        outputs = model(**inputs)

    attention_mask = inputs["attention_mask"]
    token_embeddings = outputs.last_hidden_state
    mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
    sum_embeddings = torch.sum(token_embeddings * mask_expanded, dim=1)
    sum_mask = torch.clamp(mask_expanded.sum(dim=1), min=1e-9)
    embeddings = sum_embeddings / sum_mask
    embeddings = F.normalize(embeddings, p=2, dim=1)

    return embeddings.cpu().tolist()


# --- Modal remote GPU inference ---

def embed_batch_modal(texts: list[str]) -> list[list[float]]:
    import modal

    Embedder = modal.Cls.from_name("newslens-embedder", "Embedder")
    embedder = Embedder()
    return embedder.embed.remote(texts)


# --- Main ---

def main():
    s3 = get_s3_client()

    # Read input
    obj = s3.get_object(Bucket=STORAGE_BUCKET, Key=INPUT_KEY)
    lines = obj["Body"].read().decode("utf-8").strip().splitlines()
    articles = [json.loads(line) for line in lines]
    logger.info("Read %d articles from s3://%s/%s", len(articles), STORAGE_BUCKET, INPUT_KEY)

    # Prepare texts with E5 prefix
    texts = [f"passage: {a['title']}{a['body']}" for a in articles]

    # Load local model if not using Modal
    if USE_MODAL:
        logger.info("Using Modal remote GPU for inference")
    else:
        tokenizer, model, device = load_model()

    # Embed in batches
    results = []
    for i in range(0, len(texts), BATCH_SIZE):
        batch = texts[i : i + BATCH_SIZE]

        if USE_MODAL:
            embeddings = embed_batch_modal(batch)
        else:
            embeddings = embed_batch_local(batch, tokenizer, model, device)

        for j, emb in enumerate(embeddings):
            results.append({
                "article_id": articles[i + j]["article_id"],
                "embedding": emb,
            })
        logger.info("Embedded batch %d/%d", i // BATCH_SIZE + 1, (len(texts) - 1) // BATCH_SIZE + 1)

    # Write output
    ndjson = "\n".join(json.dumps(r) for r in results)
    # Derive output key from input: cleaned/2026-08-15/articles.ndjson -> embeddings/2026-08-15/embeddings.ndjson
    date_part = INPUT_KEY.split("/")[1]
    out_key = f"embeddings/{date_part}/embeddings.ndjson"
    s3.put_object(Bucket=STORAGE_BUCKET, Key=out_key, Body=ndjson.encode("utf-8"))

    logger.info("Wrote %d embeddings to s3://%s/%s", len(results), STORAGE_BUCKET, out_key)
    print(out_key)


if __name__ == "__main__":
    main()
