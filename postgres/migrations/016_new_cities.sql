-- Migration 016: Add Köln, München, Hamburg, Berlin

INSERT INTO cities (id, slug, name, emoji, subtitle, country_code, is_published, sort_order)
VALUES
  (4, 'koeln',   'Köln',    '🏛️', 'Rheinische Gastfreundschaft trifft kulinarische Vielfalt.',   'DE', TRUE, 4),
  (5, 'muenchen','München',  '🥨', 'Zwischen Biergarten und Sternegastronomie — Münchner Tische.', 'DE', TRUE, 5),
  (6, 'hamburg', 'Hamburg',  '⚓', 'Fischmarkt, Hafenflair und die besten Adressen der Stadt.',    'DE', TRUE, 6),
  (7, 'berlin',  'Berlin',   '🐻', 'Die aufregendste Foodszene Deutschlands — ehrlich kuratiert.', 'DE', TRUE, 7)
ON CONFLICT (slug) DO UPDATE SET is_published = TRUE;

SELECT setval('cities_id_seq', GREATEST((SELECT MAX(id) FROM cities), 7));
