"""
Pydantic schemas for the NDJSON contracts between pipeline stages.

Each container writes NDJSON where every line must conform to a schema.
These models enforce the contract so that a breaking change in stage N
is caught before it silently corrupts stage N+1.

Pipeline flow:
    scraper  ->  ScraperOutput
    cleaner  ->  CleanerOutput   (consumed by embedder + bias classifiers)
    embedder ->  EmbedderOutput  (consumed by bias-xgb + clustering + loader)
    bias-*   ->  BiasOutput      (consumed by loader)
    cluster  ->  ClusterOutput   (consumed by loader)
"""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, field_validator


# ---------- Scraper -> Cleaner ----------

class ScraperOutput(BaseModel):
    """One line of the scraper's NDJSON output (raw/YYYY-MM-DD/articles.ndjson)."""
    article_id: str = Field(min_length=24, max_length=24)
    source: str = Field(min_length=1)
    url: str = Field(min_length=1)
    title: str
    body: str
    language: str
    published_at: str | None = None
    scraped_at: str


# ---------- Cleaner -> Embedder / Bias ----------

class CleanerOutput(BaseModel):
    """One line of the cleaner's NDJSON output (cleaned/YYYY-MM-DD/articles.ndjson).

    Same shape as ScraperOutput but with normalized text fields.
    """
    article_id: str = Field(min_length=24, max_length=24)
    source: str = Field(min_length=1)
    url: str = Field(min_length=1)
    title: str
    body: str = Field(min_length=1)
    language: str
    published_at: str | None = None
    scraped_at: str


# ---------- Embedder -> Bias-XGB / Clustering / Loader ----------

class EmbedderOutput(BaseModel):
    """One line of the embedder's NDJSON output (embeddings/YYYY-MM-DD/embeddings.ndjson)."""
    article_id: str = Field(min_length=24, max_length=24)
    embedding: list[float]

    @field_validator("embedding")
    @classmethod
    def embedding_not_empty(cls, v: list[float]) -> list[float]:
        if len(v) == 0:
            raise ValueError("embedding must not be empty")
        return v


# ---------- Bias classifiers (XGB + Transformers) -> Loader ----------

BIAS_LABELS = Literal["far_left", "left", "center", "right", "far_right"]


class BiasOutput(BaseModel):
    """One line of the bias classifier's NDJSON output (bias/YYYY-MM-DD/bias_results.ndjson).

    Both XGBoost and HelaBERT transformers containers produce this same schema.
    """
    article_id: str = Field(min_length=24, max_length=24)
    bias_label: BIAS_LABELS
    bias_confidence: float = Field(ge=0.0, le=1.0)
    bias_scores: dict[BIAS_LABELS, float]

    @field_validator("bias_scores")
    @classmethod
    def scores_have_all_labels(cls, v: dict) -> dict:
        expected = {"far_left", "left", "center", "right", "far_right"}
        if set(v.keys()) != expected:
            raise ValueError(f"bias_scores keys must be {expected}, got {set(v.keys())}")
        return v


# ---------- Clustering -> Loader ----------

CLUSTER_METHODS = Literal["knn", "hdbscan_new", "single"]


class ClusterOutput(BaseModel):
    """One line of the clustering NDJSON output (clusters/YYYY-MM-DD/cluster_assignments.ndjson)."""
    article_id: str = Field(min_length=24, max_length=24)
    event_id: str = Field(min_length=1)
    cluster_method: CLUSTER_METHODS
    distance: float = Field(ge=0.0)
