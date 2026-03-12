-- Migration 002: Gemini pre-qualification table
-- Safe to run multiple times (IF NOT EXISTS guards)

CREATE TABLE IF NOT EXISTS gemini_prequalify (
    place_id     TEXT PRIMARY KEY,
    unique_score SMALLINT CHECK (unique_score BETWEEN 1 AND 10),
    foodie_score SMALLINT CHECK (foodie_score BETWEEN 1 AND 10),
    qualified    BOOLEAN  NOT NULL DEFAULT FALSE,
    rejected     BOOLEAN  NOT NULL DEFAULT FALSE,
    evaluated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_prequalify_candidates
    ON gemini_prequalify (qualified, rejected)
    WHERE qualified = TRUE AND rejected = FALSE;
