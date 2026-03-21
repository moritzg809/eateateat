-- Migration 010: Curated restaurant collections ("Listen")
-- Each collection defines a filter rule that maps to existing DB fields.
-- filter_type IN ('audience','cuisine','profile','city','search','tag')

CREATE TABLE IF NOT EXISTS collections (
    id           SERIAL PRIMARY KEY,
    slug         TEXT NOT NULL UNIQUE,
    title        TEXT NOT NULL,
    subtitle     TEXT,                     -- Short description shown on the tile
    emoji        TEXT DEFAULT '🍽',
    filter_type  TEXT NOT NULL CHECK (filter_type IN (
                     'audience',           -- e.audience_type = filter_value
                     'cuisine',            -- e.cuisine_type ILIKE %filter_value%
                     'profile',            -- e.{filter_value}_score >= min_score
                     'city',               -- r.address ILIKE %filter_value%
                     'search',             -- name/cuisine/summary_de ILIKE %filter_value%
                     'tag'                 -- filter_value IN cuisine_tags/food_tags
                 )),
    filter_value TEXT NOT NULL,
    min_score    SMALLINT DEFAULT 7,       -- For profile type: minimum score threshold
    is_published BOOLEAN NOT NULL DEFAULT FALSE,
    sort_order   INTEGER DEFAULT 0,
    created_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_collections_slug        ON collections(slug);
CREATE INDEX IF NOT EXISTS idx_collections_published   ON collections(is_published, sort_order);

-- ── Seed Data ────────────────────────────────────────────────────────────────

INSERT INTO collections (slug, title, subtitle, emoji, filter_type, filter_value, min_score, is_published, sort_order) VALUES
  ('mallorquinische-kueche',
   'Mallorquinische Küche',
   'Authentische Inselküche — von Sobrasada bis Tumbet',
   '🥘', 'cuisine', 'Mallorquin', 7, TRUE, 1),

  ('romantische-abende',
   'Romantische Abende',
   'Die schönsten Tische für zwei — mit Stimmung, Kerzen und Meerblick',
   '💑', 'profile', 'date', 8, TRUE, 2),

  ('geheimtipps',
   'Geheimtipps',
   'Entdeckungen abseits der Touristenpfade — so wie Einheimische essen',
   '💎', 'audience', 'local', 7, TRUE, 3),

  ('gourmet',
   'Gourmet',
   'Spitzenküche auf der Insel — für besondere Anlässe',
   '🌟', 'audience', 'gourmet', 7, TRUE, 4),

  ('strandrestaurants',
   'Strandrestaurants',
   'Füße im Sand, Blick aufs Meer — die besten Chiringuitos',
   '🌊', 'search', 'Chiringuito', 7, TRUE, 5),

  ('fruehstueck-brunch',
   'Frühstück & Brunch',
   'Grandiose Morgenstunden mit Café con Leche und frischen Zutaten',
   '☕', 'search', 'Brunch', 7, TRUE, 6),

  ('weinbars-bodegas',
   'Weinbars & Bodegas',
   'Mallorquinische Weine, natürliche Tropfen und kleine Happen',
   '🍷', 'cuisine', 'Weinbar', 7, TRUE, 7),

  ('tramuntana',
   'Tramuntana & Berge',
   'Restaurants in der UNESCO-Bergwelt rund um Sóller und Valldemossa',
   '🏔', 'city', 'Sóller', 7, TRUE, 8),

  ('santa-catalina',
   'Santa Catalina',
   'Das hipste Viertel Palmas — Markthalle, Naturwein und moderne Küche',
   '🌆', 'search', 'Santa Catalina', 7, TRUE, 9),

  ('farm-to-table',
   'Farm to Table',
   'Direkt vom mallorquinischen Feld auf den Teller',
   '🌿', 'search', 'Farm', 7, TRUE, 10)

ON CONFLICT (slug) DO NOTHING;
