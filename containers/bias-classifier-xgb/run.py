"""
Bias classifier container (XGBoost) entry point.

Reads pre-computed embeddings NDJSON from MinIO, classifies bias using a
trained XGBoost model, and writes bias results NDJSON back to MinIO.
Prints the output key to stdout for Airflow to capture via XCom.

Environment variables:
    INPUT_KEY             - MinIO key for embeddings NDJSON
    STORAGE_ENDPOINT      - S3/MinIO endpoint URL
    STORAGE_BUCKET        - bucket name
    AWS_ACCESS_KEY_ID     - S3/MinIO access key
    AWS_SECRET_ACCESS_KEY - S3/MinIO secret key
    MODEL_KEY             - S3 key for the XGBoost model file (e.g. models/bias-classifier-xgb/xgb_model.joblib)
"""
from __future__ import annotations

import json
import logging
import os
import tempfile

import boto3
import joblib
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

INPUT_KEY = os.environ["INPUT_KEY"]
STORAGE_ENDPOINT = os.environ["STORAGE_ENDPOINT"]
STORAGE_BUCKET = os.environ["STORAGE_BUCKET"]
MODEL_KEY = os.environ["MODEL_KEY"]

LABELS = ["far_left", "left", "center", "right", "far_right"]


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=STORAGE_ENDPOINT,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )


def load_model(s3):
    """Download XGBoost model from MinIO and load it."""
    logger.info("Downloading model from s3://%s/%s", STORAGE_BUCKET, MODEL_KEY)
    with tempfile.NamedTemporaryFile(suffix=".joblib") as tmp:
        s3.download_file(STORAGE_BUCKET, MODEL_KEY, tmp.name)
        model = joblib.load(tmp.name)
    logger.info("Model loaded")
    return model


def main():
    s3 = get_s3_client()

    # Read embeddings
    obj = s3.get_object(Bucket=STORAGE_BUCKET, Key=INPUT_KEY)
    lines = obj["Body"].read().decode("utf-8").strip().splitlines()
    records = [json.loads(line) for line in lines]
    logger.info("Read %d embeddings from s3://%s/%s", len(records), STORAGE_BUCKET, INPUT_KEY)

    # Load model
    model = load_model(s3)

    # Classify
    embeddings = np.array([r["embedding"] for r in records])
    probabilities = model.predict_proba(embeddings)

    results = []
    for i, r in enumerate(records):
        probs = probabilities[i]
        scores = {label: float(probs[j]) for j, label in enumerate(LABELS)}
        top_label = max(scores, key=scores.get)
        results.append({
            "article_id": r["article_id"],
            "bias_label": top_label,
            "bias_confidence": scores[top_label],
            "bias_scores": scores,
        })

    # Write output
    ndjson = "\n".join(json.dumps(r) for r in results)
    date_part = INPUT_KEY.split("/")[1]
    out_key = f"bias/{date_part}/bias_results.ndjson"
    s3.put_object(Bucket=STORAGE_BUCKET, Key=out_key, Body=ndjson.encode("utf-8"))

    logger.info("Classified %d articles -> s3://%s/%s", len(results), STORAGE_BUCKET, out_key)
    print(out_key)


if __name__ == "__main__":
    main()
