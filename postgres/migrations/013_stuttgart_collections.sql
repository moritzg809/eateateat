-- Migration 013: Stuttgart city + curated collections ("Listen")
--
-- Inserts Stuttgart as city id=2 and seeds 10 Stuttgart-specific collections.

-- ── 1. Insert Stuttgart ───────────────────────────────────────────────────────

INSERT INTO cities (id, slug, name, emoji, subtitle, country_code, is_published, sort_order)
VALUES (2, 'stuttgart', 'Stuttgart', '🍾',
        'Die besten Tische der Schwabenmetropole — von der Besenwirtschaft bis zum Sternerestaurant.',
        'DE', TRUE, 2)
ON CONFLICT (slug) DO UPDATE SET is_published = TRUE;

-- Advance sequence so further INSERTs get id >= 3
SELECT setval('cities_id_seq', GREATEST((SELECT MAX(id) FROM cities), 2));

-- ── 2. Stuttgart Collections ──────────────────────────────────────────────────

INSERT INTO collections (slug, title, subtitle, emoji, filter_type, filter_value, min_score, is_published, sort_order, city_id) VALUES

  ('stgt-besenwirtschaften',
   'Besenwirtschaften',
   'Schwäbische Tradition pur — Trollinger aus der Region, Maultaschen und echte Gastlichkeit',
   '🍾', 'search', 'Besen', 7, TRUE, 1, 2),

  ('stgt-weinbars',
   'Weinbars & Weinstuben',
   'Württemberger Wein trifft moderne Weinbar — die besten Adressen für Weinliebhaber',
   '🍷', 'cuisine', 'Weinbar', 7, TRUE, 2, 2),

  ('stgt-romantisch',
   'Romantische Abende',
   'Die schönsten Tische zu zweit — Kerzenlicht, toller Wein und stimmungsvolle Atmosphäre',
   '💑', 'profile', 'date', 8, TRUE, 3, 2),

  ('stgt-gourmet',
   'Fine Dining & Sterne',
   'Spitzenrestaurants und Michelin-Sterne — Stuttgarts kulinarische Hochklasse',
   '🌟', 'audience', 'gourmet', 7, TRUE, 4, 2),

  ('stgt-bohnenviertel',
   'Bohnenviertel',
   'Das angesagteste Viertel der Stadt — Bistros, Weinbars und internationale Küche',
   '🌆', 'search', 'Bohnenviertel', 7, TRUE, 5, 2),

  ('stgt-brunch',
   'Frühstück & Brunch',
   'Traumhafte Morgenstunden mit Croissant, Eggs Benedict und gutem Kaffee',
   '☕', 'search', 'Brunch', 7, TRUE, 6, 2),

  ('stgt-geheimtipps',
   'Geheimtipps',
   'Die Lieblingsorte der Stuttgarter — abseits der bekannten Pfade',
   '💎', 'audience', 'local', 7, TRUE, 7, 2),

  ('stgt-vegetarisch',
   'Vegetarisch & Vegan',
   'Plant-based und pflanzenreich — die besten vegetarischen Adressen der Stadt',
   '🥗', 'search', 'Vegetarisch', 7, TRUE, 8, 2),

  ('stgt-italienisch',
   'Cucina Italiana',
   'Stuttgart liebt Italien — die besten Pasta-, Pizza- und Trattorien der Stadt',
   '🍝', 'cuisine', 'Italienisch', 7, TRUE, 9, 2),

  ('stgt-aussicht',
   'Mit Ausblick',
   'Essen mit Weitblick — Restaurants auf dem Killesberg und an den Weinsteigen',
   '🌅', 'search', 'Killesberg', 7, TRUE, 10, 2),

  ('stgt-biergaerten',
   'Biergärten',
   'Stuttgarter Sommerkultur — kühles Bier, Brezeln und lauschige Gärten unter freiem Himmel',
   '🍺', 'search', 'Biergarten', 7, TRUE, 11, 2),

  ('stgt-cocktailbars',
   'Cocktailbars',
   'Die besten Bars der Stadt — kreative Drinks, stimmungsvolles Licht und die perfekte Nacht',
   '🍸', 'search', 'Cocktail', 7, TRUE, 12, 2)

ON CONFLICT (slug) DO NOTHING;
