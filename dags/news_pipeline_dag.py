"""
News event pipeline DAG.

Orchestration only — every task delegates to include/ modules. This file
should stay cheap to import (no heavy ML libs at module scope) since Airflow
re-parses it on every scheduler heartbeat.

Schedule: every 4 hours. Adjust to your scraping cadence / source update
frequency and LLM budget — summarization + topic tagging cost scale with
run frequency, not article volume, since cost is per-event not per-article.
"""
from __future__ import annotations

import datetime as dt

from airflow.decorators import dag, task
from airflow.models.param import Param

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": dt.timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": dt.timedelta(minutes=30),
}


@dag(
    dag_id="news_event_pipeline",
    description="Scrape -> clean -> bias classify -> embed -> cluster -> summarize -> topic tag -> load",
    schedule="0 */4 * * *",
    start_date=dt.datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,          # avoid overlapping runs writing to the same clustering time window
    tags=["news", "nlp", "ml"],
    params={"run_date": Param(default=None, type=["null", "string"])},
)
def news_event_pipeline():

    @task
    def scrape(**context) -> str:
        from include.scraping.scraper import scrape_all
        run_date = context["ds"]
        return scrape_all(run_date=run_date)

    @task
    def clean(raw_key: str) -> str:
        from include.preprocessing.cleaning import clean_raw_articles
        return clean_raw_articles(raw_key)

    @task
    def classify_bias(clean_key: str) -> str:
        from include.ml.bias_classifier import classify_articles
        return classify_articles(clean_key)

    @task
    def embed(clean_key: str) -> str:
        from include.ml.embeddings import embed_articles
        return embed_articles(clean_key)

    @task
    def cluster(embeddings_key: str) -> str:
        from include.ml.clustering import cluster_articles
        return cluster_articles(embeddings_key)

    @task
    def summarize(clean_key: str, clusters_key: str) -> str:
        from include.ml.summarization import summarize_events
        return summarize_events(clean_key, clusters_key)

    @task
    def tag_topics(summaries_key: str) -> str:
        from include.ml.topic_assignment import assign_topics
        return assign_topics(summaries_key)

    @task
    def load(clean_key: str, bias_key: str, embeddings_key: str,
              clusters_key: str, summaries_key: str, topics_key: str) -> None:
        from include.db.postgres_client import load_final_results
        load_final_results(clean_key, bias_key, embeddings_key, clusters_key, summaries_key, topics_key)

    raw_key = scrape()
    clean_key = clean(raw_key)

    # bias classification and embedding both only need cleaned text -> run in parallel
    bias_key = classify_bias(clean_key)
    embeddings_key = embed(clean_key)

    clusters_key = cluster(embeddings_key)
    summaries_key = summarize(clean_key, clusters_key)
    topics_key = tag_topics(summaries_key)

    load(clean_key, bias_key, embeddings_key, clusters_key, summaries_key, topics_key)


news_event_pipeline()
