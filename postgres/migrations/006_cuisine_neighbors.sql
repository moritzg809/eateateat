-- Migration 006: cuisine_neighbors table for Jina-based semantic cuisine filtering
-- Run offline: docker compose run --rm enricher python cuisine_embed.py

CREATE TABLE IF NOT EXISTS cuisine_neighbors (
    cuisine_type   TEXT PRIMARY KEY,
    similar_types  TEXT[]      NOT NULL DEFAULT '{}',
    embedded_at    TIMESTAMPTZ DEFAULT NOW()
);
