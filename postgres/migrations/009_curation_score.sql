-- Migration 009: Add curation_score column to gemini_enrichments
-- Pre-computed editorial ranking score (0–100) used as default index sort order.
-- Formula: Critic (25%) + Quality (20%) + Bayesian Rating (30%) + Completeness (15%) + Audience (10%)

ALTER TABLE gemini_enrichments
  ADD COLUMN IF NOT EXISTS curation_score NUMERIC(5,2);

CREATE INDEX IF NOT EXISTS idx_enrichments_curation_score
  ON gemini_enrichments(curation_score DESC NULLS LAST);
