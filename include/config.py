"""
Central configuration, read once per task via Airflow Variables / env vars.

Keep this the single source of truth for connection IDs, model paths, and
tunables so DAG code and include/ modules never hardcode strings twice.
"""
import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Config:
    # --- Airflow connection IDs (create these in Airflow UI / infra IaC, not here) ---
    postgres_conn_id: str = "news_postgres"
    vector_db_conn_id: str = "news_vector_db"   # if using external vector DB (e.g. Pinecone/Weaviate/Qdrant)
    object_storage_conn_id: str = "news_object_storage"  # S3 / GCS / ADLS

    # --- Scraping ---
    sources_config_path: str = "include/scraping/sources.yaml"
    scrape_timeout_s: int = 20
    scrape_concurrency: int = 8
    raw_html_prefix: str = "raw/{run_date}/"       # object storage key prefix

    # --- Preprocessing ---
    min_article_chars: int = 200
    dedupe_similarity_threshold: float = 0.92       # near-duplicate detection (minhash / embedding cosine)
    allowed_languages: tuple = ("en",)

    # --- Bias classification ---
    bias_model_uri: str = os.environ.get("BIAS_MODEL_URI", "s3://news-pipeline-models/bias-classifier/latest")
    bias_labels: tuple = ("left", "center", "right")   # adjust to your label taxonomy
    bias_batch_size: int = 32

    # --- Embeddings ---
    embedding_model_name: str = os.environ.get("EMBEDDING_MODEL", "BAAI/bge-base-en-v1.5")
    embedding_dim: int = 768
    embedding_batch_size: int = 64

    # --- Clustering ---
    cluster_window_hours: int = 48          # only cluster articles within a rolling time window
    hdbscan_min_cluster_size: int = 3
    hdbscan_min_samples: int = 2
    hdbscan_metric: str = "euclidean"       # note: HDBSCAN needs euclidean; cosine-normalize embeddings first

    # --- Summarization / topic assignment ---
    llm_model: str = os.environ.get("SUMMARIZATION_MODEL", "claude-sonnet-4-6")
    llm_max_tokens: int = 1024
    topic_taxonomy_path: str = "include/ml/topics.yaml"   # None => let the LLM propose free-form topics

    # --- Misc ---
    env: str = os.environ.get("PIPELINE_ENV", "dev")   # dev | staging | prod


CONFIG = Config()
