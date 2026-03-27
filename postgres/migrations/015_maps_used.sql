-- Track whether a Gemini enrichment used the google_maps tool.
-- Existing rows are assumed to have used maps (conservative / safe default).
-- Only enrich.py sets this to FALSE when url_context was used instead.
ALTER TABLE gemini_enrichments
    ADD COLUMN IF NOT EXISTS maps_used BOOLEAN NOT NULL DEFAULT TRUE;
