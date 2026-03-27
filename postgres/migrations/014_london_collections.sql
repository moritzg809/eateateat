-- Migration 014: London city + curated collections ("Listen")

-- ── 1. Insert London ──────────────────────────────────────────────────────────

INSERT INTO cities (id, slug, name, emoji, subtitle, country_code, is_published, sort_order)
VALUES (3, 'london', 'London', '🇬🇧',
        'The best tables in the city — from gastropubs to Michelin stars.',
        'GB', TRUE, 3)
ON CONFLICT (slug) DO UPDATE SET is_published = TRUE;

SELECT setval('cities_id_seq', GREATEST((SELECT MAX(id) FROM cities), 3));

-- ── 2. London Collections ─────────────────────────────────────────────────────

INSERT INTO collections (slug, title, subtitle, emoji, filter_type, filter_value, min_score, is_published, sort_order, city_id) VALUES

  ('lon-gastropubs',
   'Gastropubs',
   'The great British pub — elevated. Proper food, cask ales and that unmistakable atmosphere',
   '🍺', 'search', 'Gastropub', 7, TRUE, 1, 3),

  ('lon-sunday-roast',
   'Sunday Roast',
   'The ultimate London ritual — crispy roasties, Yorkshire pudding and all the trimmings',
   '🥩', 'search', 'Sunday Roast', 7, TRUE, 2, 3),

  ('lon-fine-dining',
   'Fine Dining & Michelin',
   'London''s world-class restaurant scene — tasting menus, starred kitchens and unforgettable evenings',
   '🌟', 'audience', 'gourmet', 7, TRUE, 3, 3),

  ('lon-romantic',
   'Romantic Evenings',
   'The most intimate tables in the city — candlelight, great wine and the perfect date night',
   '💑', 'profile', 'date', 8, TRUE, 4, 3),

  ('lon-afternoon-tea',
   'Afternoon Tea',
   'Finger sandwiches, scones with clotted cream and the finest loose-leaf teas',
   '🫖', 'search', 'Afternoon Tea', 7, TRUE, 5, 3),

  ('lon-natural-wine',
   'Natural Wine Bars',
   'Lo-fi wines, small producers and the buzzing bars that put London on the natural wine map',
   '🍷', 'search', 'Natural Wine', 7, TRUE, 6, 3),

  ('lon-brunch',
   'Brunch',
   'London does brunch like nowhere else — from neighbourhood cafés to all-day spots',
   '☕', 'search', 'Brunch', 7, TRUE, 7, 3),

  ('lon-hidden-gems',
   'Hidden Gems',
   'The places locals love and tourists rarely find — under the radar, over the top',
   '💎', 'audience', 'local', 7, TRUE, 8, 3),

  ('lon-indian',
   'Indian & South Asian',
   'From Brick Lane to Mayfair — London''s extraordinary Indian dining scene',
   '🍛', 'search', 'Indian', 7, TRUE, 9, 3),

  ('lon-east-london',
   'East London',
   'Shoreditch, Hackney, Dalston — the neighbourhood that defines London''s food culture',
   '🌆', 'search', 'Shoreditch', 7, TRUE, 10, 3),

  ('lon-cocktail-bars',
   'Cocktail Bars',
   'World-class bartending in the city that invented the speakeasy revival',
   '🍸', 'search', 'Cocktail', 7, TRUE, 11, 3),

  ('lon-rooftop',
   'Rooftop & Views',
   'Eat and drink above the skyline — London from its best angle',
   '🌅', 'search', 'Rooftop', 7, TRUE, 12, 3)

ON CONFLICT (slug) DO NOTHING;
