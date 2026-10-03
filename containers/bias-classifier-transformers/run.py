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
    MODEL_REPO            - HuggingFace repo ID (default: sychpra/helabert-bias-classifier)
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
STORAGE_ENDPOINT = os.environ.get("STORAGE_ENDPOINT")  # None = real AWS S3
STORAGE_BUCKET = os.environ["STORAGE_BUCKET"]
MODEL_REPO = os.environ.get("MODEL_REPO", "sychpra/helabert-bias-classifier")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "32"))
USE_MODAL = os.environ.get("USE_MODAL", "false").lower() == "true"
MAX_LENGTH = 512
NUM_LABELS = 5

LABELS = ["far_left", "left", "center", "right", "far_right"]


def get_s3_client():
    if STORAGE_ENDPOINT:
        # Local dev: explicit MinIO credentials
        return boto3.client(
            "s3",
            endpoint_url=STORAGE_ENDPOINT,
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        )
    # AWS: uses IAM role credentials automatically
    return boto3.client("s3")


# --- Local GPU inference ---

def load_model():
    import torch
    import torch.nn as nn
    from huggingface_hub import hf_hub_download
    from transformers import BertModel

    logger.info("Loading HelaBERT model locally from: %s", MODEL_REPO)

    # Download files from HF Hub
    bert_dir = hf_hub_download(MODEL_REPO, "bert/config.json")
    bert_dir = bert_dir.rsplit("/", 1)[0]
    hf_hub_download(MODEL_REPO, "bert/model.safetensors")
    sp_path = hf_hub_download(MODEL_REPO, "tokenizer/unigram_32000_0.9995.model")
    head_path = hf_hub_download(MODEL_REPO, "classifier_head.pt")

    # Load SentencePiece tokenizer
    import sentencepiece as spm
    sp = spm.SentencePieceProcessor()
    sp.Load(sp_path)

    # Load BERT encoder
    bert = BertModel.from_pretrained(bert_dir)

    # Build full model
    class HelaBERTClassifier(nn.Module):
        def __init__(self, bert, num_labels=NUM_LABELS):
            super().__init__()
            self.bert = bert
            self.dropout = nn.Dropout(0.1)
            self.classifier = nn.Linear(768, num_labels)

        def forward(self, input_ids, attention_mask):
            outputs = self.bert(input_ids=input_ids, attention_mask=attention_mask)
            hidden_states = outputs.last_hidden_state
            mask = attention_mask.unsqueeze(-1).float()
            pooled = (hidden_states * mask).sum(dim=1) / mask.sum(dim=1).clamp(min=1e-9)
            pooled = self.dropout(pooled)
            return self.classifier(pooled)

    model = HelaBERTClassifier(bert)
    model.classifier.load_state_dict(torch.load(head_path, map_location="cpu"))
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model.to(device).eval()
    logger.info("Model loaded on %s", device)
    return sp, model, device


def tokenize_batch(texts: list[str], sp, device) -> tuple:
    import torch

    all_ids = []
    all_masks = []
    for text in texts:
        ids = sp.encode(text, out_type=int)
        ids = ids[:MAX_LENGTH]
        mask = [1] * len(ids)
        pad_len = MAX_LENGTH - len(ids)
        ids += [sp.pad_id()] * pad_len
        mask += [0] * pad_len
        all_ids.append(ids)
        all_masks.append(mask)

    return (
        torch.tensor(all_ids, dtype=torch.long).to(device),
        torch.tensor(all_masks, dtype=torch.long).to(device),
    )


def classify_batch_local(texts: list[str], sp, model, device) -> list[list[float]]:
    import torch

    input_ids, attention_mask = tokenize_batch(texts, sp, device)

    with torch.no_grad():
        logits = model(input_ids, attention_mask)
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
        sp, model, device = load_model()

    # Classify in batches
    results = []
    for i in range(0, len(texts), BATCH_SIZE):
        batch = texts[i : i + BATCH_SIZE]

        if USE_MODAL:
            batch_probs = classify_batch_modal(batch)
        else:
            batch_probs = classify_batch_local(batch, sp, model, device)

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
