-- Jina-computed nearest neighbors for cuisine_tags / interior_tags / food_tags values.
-- Populated by scraper/attr_embed.py.  Used by Flask to expand ?attr_filter queries.
CREATE TABLE IF NOT EXISTS attr_neighbors (
    attr_value    TEXT        PRIMARY KEY,
    similar_attrs TEXT[]      NOT NULL DEFAULT '{}',
    embedded_at   TIMESTAMPTZ DEFAULT NOW()
);
