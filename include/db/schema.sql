-- Run once (e.g. via an Airflow task using PostgresOperator, or a migration
-- tool like alembic) to set up the target schema. Requires the pgvector
-- extension if you're using Postgres as the vector store instead of an
-- external vector DB (Pinecone/Weaviate/Qdrant).

CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pgcrypto;  -- for gen_random_uuid()

CREATE TABLE IF NOT EXISTS sources (
    source_name   TEXT PRIMARY KEY,
    source_type   TEXT NOT NULL,           -- 'rss' | 'html'
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS articles (
    article_id     TEXT PRIMARY KEY,        -- sha256(url)[:24], matches include/scraping/scraper.py
    source_name    TEXT NOT NULL REFERENCES sources(source_name),
    url            TEXT NOT NULL UNIQUE,
    title          TEXT NOT NULL,
    body           TEXT NOT NULL,
    language       TEXT NOT NULL,
    published_at   TIMESTAMPTZ,
    scraped_at     TIMESTAMPTZ NOT NULL,
    bias_label     TEXT,
    bias_confidence REAL,
    bias_scores    JSONB,
    embedding      vector(768),             -- must match CONFIG.embedding_dim
    event_id       UUID,                    -- nullable: NULL if HDBSCAN labeled it noise
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_articles_event_id ON articles(event_id);
CREATE INDEX IF NOT EXISTS idx_articles_published_at ON articles(published_at);
CREATE INDEX IF NOT EXISTS idx_articles_source ON articles(source_name);
-- ivfflat requires ANALYZE after enough rows exist; adjust lists as the table grows
CREATE INDEX IF NOT EXISTS idx_articles_embedding ON articles USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

CREATE TABLE IF NOT EXISTS events (
    event_id       UUID PRIMARY KEY,
    summary        TEXT NOT NULL,
    topic          TEXT NOT NULL,
    article_count  INT NOT NULL,
    source_count   INT NOT NULL,
    window_start   TIMESTAMPTZ,             -- earliest article's published_at in this cluster
    window_end     TIMESTAMPTZ,             -- latest article's published_at in this cluster
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_events_topic ON events(topic);
CREATE INDEX IF NOT EXISTS idx_events_window ON events(window_start, window_end);

ALTER TABLE articles
    ADD CONSTRAINT fk_articles_event
    FOREIGN KEY (event_id) REFERENCES events(event_id)
    DEFERRABLE INITIALLY DEFERRED;
    -- deferred because articles are loaded before events exist within the same load task;
    -- alternatively load events first, then articles, and drop DEFERRABLE.
