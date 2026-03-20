-- Migration 008: Editorial articles table for "Unsere Tipps" blog feature
CREATE TABLE IF NOT EXISTS editorial_articles (
    id SERIAL PRIMARY KEY,
    place_id TEXT NOT NULL UNIQUE REFERENCES restaurants(place_id) ON DELETE CASCADE,
    slug TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    article_md TEXT NOT NULL,
    teaser TEXT,
    gemini_model TEXT,
    generated_at TIMESTAMPTZ DEFAULT NOW(),
    is_published BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX idx_editorial_articles_place_id ON editorial_articles(place_id);
CREATE INDEX idx_editorial_articles_slug ON editorial_articles(slug);
CREATE INDEX idx_editorial_articles_published ON editorial_articles(is_published);
