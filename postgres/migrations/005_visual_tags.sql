-- Migration 005: visual tag arrays for interior style and food presentation
-- Added 2026-03-13 — backfill via critic_enrich.py --backfill

ALTER TABLE gemini_enrichments
    ADD COLUMN IF NOT EXISTS interior_tags TEXT[],   -- 5 Schlagworte zum Einrichtungsstil
    ADD COLUMN IF NOT EXISTS food_tags     TEXT[];   -- 5 Schlagworte zur Speisen-Optik
