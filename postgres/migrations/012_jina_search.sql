-- Migration 012: Add Jina semantic search embedding columns
-- Adds jina_text + jina_embedding to restaurant_embeddings so the pipeline
-- can store per-restaurant search vectors (jina-embeddings-v3, 1024 dims)
-- and Flask can load them for cosine-similarity search at query time.

ALTER TABLE restaurant_embeddings
    ADD COLUMN IF NOT EXISTS jina_text        TEXT,
    ADD COLUMN IF NOT EXISTS jina_embedding   REAL[],
    ADD COLUMN IF NOT EXISTS jina_model       TEXT,
    ADD COLUMN IF NOT EXISTS jina_embedded_at TIMESTAMPTZ;
