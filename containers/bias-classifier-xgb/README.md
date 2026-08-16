# Bias Classifier Container (XGBoost)

Reads pre-computed E5 embeddings from object storage, classifies media bias using a trained XGBoost model, and writes bias results NDJSON back to object storage.

## What it does

1. **Reads** embeddings NDJSON from MinIO (output of the embedder stage)
2. **Downloads** the XGBoost model (`.joblib`) from MinIO
3. **Classifies** each article into one of 5 bias labels using `predict_proba`
4. **Writes** `{article_id, bias_label, bias_confidence, bias_scores}` NDJSON back to MinIO

## I/O Contract

**Input** (from embedder): `embeddings/{RUN_DATE}/embeddings.ndjson`
```json
{"article_id": "...", "embedding": [0.024, -0.010, ...]}
```

**Output**: `bias/{RUN_DATE}/bias_results.ndjson`
```json
{"article_id": "...", "bias_label": "center", "bias_confidence": 0.82, "bias_scores": {"far_left": 0.01, "left": 0.05, "center": 0.82, "right": 0.10, "far_right": 0.02}}
```

## Labels

`far_left`, `left`, `center`, `right`, `far_right`

## Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `INPUT_KEY` | Yes | S3 key of embeddings NDJSON (e.g. `embeddings/2026-08-15/embeddings.ndjson`) |
| `STORAGE_ENDPOINT` | Yes | S3/MinIO endpoint |
| `STORAGE_BUCKET` | Yes | Bucket name |
| `AWS_ACCESS_KEY_ID` | Yes | S3/MinIO access key |
| `AWS_SECRET_ACCESS_KEY` | Yes | S3/MinIO secret key |
| `MODEL_KEY` | Yes | S3 key of the XGBoost model (e.g. `models/bias-classifier-xgb/xgb_model.joblib`) |

## Build and run

```bash
# Build
docker build -t newslens/bias-classifier-xgb:v1 .

# Run
docker run --rm --network newslens_pipeline_default --env-file ../../.env -e INPUT_KEY=embeddings/2026-08-15/embeddings.ndjson newslens/bias-classifier-xgb:v1
```

## Model management

### Uploading a model to MinIO

Export from Colab:
```python
import joblib
joblib.dump(xgb_model, "/content/xgb_model.joblib")

from google.colab import files
files.download("/content/xgb_model.joblib")
```

Upload to MinIO:
```python
import boto3
s3 = boto3.client("s3", endpoint_url="http://localhost:9000", aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin")
s3.upload_file("~/Downloads/xgb_model.joblib", "newslens-pipeline", "models/bias-classifier-xgb/xgb_model.joblib")
```

### Swapping to a new model

Upload the new `.joblib` to MinIO (same key or a new one), update `MODEL_KEY` in `.env` if the key changed. No container rebuild needed.

### Swapping to HelaBERT classifier

Change two Airflow Variables:
- `bias_classifier_image` → `newslens/bias-classifier-transformers:v1`
- `bias_classifier_input` → `cleaned`

No code changes required. See `containers/bias-classifier-transformers/` for the alternative container.

## Why XGBoost over a transformer

This container is lightweight (~200MB) because the heavy embedding work is done by the embedder stage. XGBoost classifies from pre-computed 1024-dim vectors, so:
- No GPU required
- Fast inference (milliseconds for 36 articles)
- Small image (no torch/transformers dependency)

The trade-off: if the embedding model changes, the XGBoost model must be retrained to match.
