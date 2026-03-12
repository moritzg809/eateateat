-- Migration 003: Quality filter on top_restaurants view
--
-- Adds a minimum of 2 strong categories (score >= 8) to appear in top_restaurants.
-- Restaurants with no enrichment data yet pass through unchanged (forward-compat).
--
-- Impact on current data: keeps ~1,188 of 1,305 complete restaurants (91%).
-- The 117 dropped restaurants score well in only 0–1 categories — they are
-- specialised/niche spots but not broadly excellent.

CREATE OR REPLACE VIEW top_restaurants AS
SELECT
    id,
    place_id,
    name,
    address,
    rating,
    rating_count,
    categories,
    phone,
    website,
    latitude,
    longitude,
    price_level,
    thumbnail_url
FROM restaurants r
WHERE pipeline_status = 'complete'
  AND is_active = TRUE
  AND (
    -- No enrichment row yet → pass-through (future-proofing)
    NOT EXISTS (
        SELECT 1 FROM gemini_enrichments e
        WHERE e.place_id = r.place_id AND e.family_score IS NOT NULL
    )
    OR (
        -- Must have at least 2 categories scoring >= 8
        SELECT (
            CASE WHEN e.family_score    >= 8 THEN 1 ELSE 0 END +
            CASE WHEN e.date_score      >= 8 THEN 1 ELSE 0 END +
            CASE WHEN e.friends_score   >= 8 THEN 1 ELSE 0 END +
            CASE WHEN e.solo_score      >= 8 THEN 1 ELSE 0 END +
            CASE WHEN e.relaxed_score   >= 8 THEN 1 ELSE 0 END +
            CASE WHEN e.party_score     >= 8 THEN 1 ELSE 0 END +
            CASE WHEN e.special_score   >= 8 THEN 1 ELSE 0 END +
            CASE WHEN e.foodie_score    >= 8 THEN 1 ELSE 0 END +
            CASE WHEN e.lingering_score >= 8 THEN 1 ELSE 0 END +
            CASE WHEN e.unique_score    >= 8 THEN 1 ELSE 0 END +
            CASE WHEN e.dresscode_score >= 8 THEN 1 ELSE 0 END
        ) >= 2
        FROM gemini_enrichments e
        WHERE e.place_id = r.place_id
    )
  )
ORDER BY rating DESC, rating_count DESC;
