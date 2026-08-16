"""
One-time loader: populates news-db with the annotated corpus from Colab.

Reads the parquet file, inserts sources, events, and articles into the database.
Noise articles (cluster_id == -1) become single-article events.

Usage:
    python3 include/db/load_corpus.py
"""
import ast
import uuid

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

DB_HOST = "localhost"
DB_PORT = 5433
DB_NAME = "news_pipeline"
DB_USER = "news"
DB_PASSWORD = "news"

PARQUET_PATH = "/home/sychpra/dev/projects/DSE-Project/newslens_pipeline/include/db/dataset.parquet"


def get_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
    )


def main():
    df = pd.read_parquet(PARQUET_PATH)
    print(f"Loaded {len(df)} articles from parquet")

    # Parse embeddings from string to list of floats
    df["embedding"] = df["embedding"].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)

    # Normalize bias labels to lowercase
    df["bias_label"] = df["bias_label"].str.lower().str.replace(" ", "_")

    conn = get_connection()
    cur = conn.cursor()

    # 1. Insert sources
    publishers = df["publisher"].unique()
    source_rows = [(p, "html") for p in publishers]
    execute_values(cur, """
        INSERT INTO sources (source_name, source_type)
        VALUES %s
        ON CONFLICT (source_name) DO NOTHING
    """, source_rows)
    print(f"Inserted {len(publishers)} sources")

    # 2. Build event mapping: cluster_id -> event UUID
    # Noise articles (cluster_id == -1) each get their own event
    event_map = {}
    for cluster_id in df["cluster_id"].unique():
        if cluster_id == -1:
            continue
        event_map[cluster_id] = str(uuid.uuid4())

    # Assign noise articles individual event UUIDs
    noise_mask = df["cluster_id"] == -1
    noise_event_ids = {idx: str(uuid.uuid4()) for idx in df[noise_mask].index}

    # 3. Insert events
    event_rows = []

    # Clustered events
    for cluster_id, event_id in event_map.items():
        cluster_df = df[df["cluster_id"] == cluster_id]
        article_count = len(cluster_df)
        source_count = cluster_df["publisher"].nunique()
        published_dates = pd.to_datetime(cluster_df["published_at"], errors="coerce")
        window_start = published_dates.min()
        window_end = published_dates.max()
        event_rows.append((
            event_id, None, None, article_count, source_count,
            None if pd.isna(window_start) else str(window_start),
            None if pd.isna(window_end) else str(window_end),
        ))

    # Noise events (single-article each)
    for idx, event_id in noise_event_ids.items():
        row = df.loc[idx]
        published = pd.to_datetime(row["published_at"], errors="coerce")
        event_rows.append((
            event_id, None, None, 1, 1,
            None if pd.isna(published) else str(published),
            None if pd.isna(published) else str(published),
        ))

    execute_values(cur, """
        INSERT INTO events (event_id, summary, topic, article_count, source_count, window_start, window_end)
        VALUES %s
        ON CONFLICT (event_id) DO NOTHING
    """, event_rows)
    print(f"Inserted {len(event_rows)} events ({len(event_map)} clusters + {len(noise_event_ids)} noise)")

    # 4. Insert articles
    article_rows = []
    for idx, row in df.iterrows():
        if row["cluster_id"] == -1:
            event_id = noise_event_ids[idx]
        else:
            event_id = event_map[row["cluster_id"]]

        embedding = row["embedding"]
        embedding_str = "[" + ",".join(str(v) for v in embedding) + "]"

        published = pd.to_datetime(row["published_at"], errors="coerce")

        article_rows.append((
            row["article_id"],
            row["publisher"],
            row["url"],
            row["title"],
            row["text"],
            "si",
            None if pd.isna(published) else str(published),
            None,  # scraped_at
            row["bias_label"],
            None,  # bias_confidence
            None,  # bias_scores
            embedding_str,
            event_id,
        ))

    execute_values(cur, """
        INSERT INTO articles (
            article_id, source_name, url, title, body, language,
            published_at, scraped_at, bias_label, bias_confidence,
            bias_scores, embedding, event_id
        )
        VALUES %s
        ON CONFLICT (article_id) DO NOTHING
    """, article_rows)
    print(f"Inserted {len(article_rows)} articles")

    conn.commit()
    cur.close()
    conn.close()
    print("Done")


if __name__ == "__main__":
    main()
