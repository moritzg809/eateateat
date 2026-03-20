"""
Generate Deep Research editorial articles for selected restaurants.

Usage:
  python generate_article.py "Miceli"
  python generate_article.py --place_id ChIJxxx
  python generate_article.py --batch  # runs for all non-generated restaurants in TARGETS list
"""
import argparse
import os
import re
import time

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from google import genai

load_dotenv()

GEMINI_MODEL = "deep-research-pro-preview-12-2025"

PROMPT_TEMPLATE = """
Schreibe einen hochwertigen deutschen Restaurantartikel über das Restaurant "{name}" in {city}, Mallorca.

Recherchiere:
- Der Küchenchef / die Gründer: Biografie, Ausbildung, Küchenstil, Philosophie
- Das Restaurant selbst: Geschichte, Atmosphäre, Lage, Auszeichnungen
- Die Küche: typische Gerichte, Konzept, lokale Tradition
- Praktische Infos: Preise, Reservierung, Öffnungszeiten
- Was das Restaurant für deutschsprachige Mallorca-Reisende besonders macht

Zielgruppe: deutschsprachige Reisende, die authentische Küche abseits des Massentourismus suchen.
Format: journalistischer Restaurantartikel mit einem prägnanten Titel (als H1), Einleitung, mehreren Abschnitten und Fazit.
Der Artikel soll zwischen 600 und 1200 Wörter lang sein.
"""


def slugify(name: str, city: str) -> str:
    parts = f"{name}-{city}".lower()
    parts = re.sub(r"[àáâãäå]", "a", parts)
    parts = re.sub(r"[èéêë]", "e", parts)
    parts = re.sub(r"[ìíîï]", "i", parts)
    parts = re.sub(r"[òóôõö]", "o", parts)
    parts = re.sub(r"[ùúûü]", "u", parts)
    parts = re.sub(r"[ñ]", "n", parts)
    parts = re.sub(r"[ç]", "c", parts)
    parts = re.sub(r"[^a-z0-9]+", "-", parts)
    return parts.strip("-")


def extract_title(article_md: str) -> str:
    for line in article_md.splitlines():
        line = line.strip()
        if line.startswith("# "):
            return line[2:].strip()
    # fallback: first non-empty line
    for line in article_md.splitlines():
        if line.strip():
            return line.strip()[:100]
    return "Unser Tipp"


def extract_teaser(article_md: str) -> str:
    """Extract first real paragraph after the title as teaser."""
    lines = article_md.splitlines()
    in_intro = False
    paragraphs = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("#"):
            if in_intro and paragraphs:
                break
            in_intro = True
            continue
        if not stripped:
            continue
        if stripped.startswith("*") or stripped.startswith("|") or stripped.startswith("-"):
            continue
        paragraphs.append(stripped)
        if len(paragraphs) >= 2:
            break
    teaser = " ".join(paragraphs)
    return teaser[:300] + ("…" if len(teaser) > 300 else "")


def extract_city(address: str) -> str:
    match = re.search(r"\d{5}\s+([^,]+)", address)
    if match:
        return match.group(1).strip()
    parts = [p.strip() for p in address.split(",")]
    return parts[-2] if len(parts) >= 2 else parts[0]


def get_connection():
    url = os.environ.get("DATABASE_URL", "postgresql://mallorcaeat:mallorcaeat_dev@localhost:5433/mallorcaeat")
    return psycopg2.connect(url)


def find_restaurant(conn, name: str = None, place_id: str = None):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        if place_id:
            cur.execute("SELECT place_id, name, address FROM restaurants WHERE place_id = %s", (place_id,))
        else:
            cur.execute(
                "SELECT place_id, name, address FROM restaurants WHERE name ILIKE %s ORDER BY rating_count DESC LIMIT 1",
                (f"%{name}%",),
            )
        return cur.fetchone()


def article_exists(conn, place_id: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM editorial_articles WHERE place_id = %s", (place_id,))
        return cur.fetchone() is not None


def save_article(conn, place_id: str, slug: str, title: str, article_md: str, teaser: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO editorial_articles (place_id, slug, title, article_md, teaser, gemini_model, is_published)
            VALUES (%s, %s, %s, %s, %s, %s, FALSE)
            ON CONFLICT (place_id) DO UPDATE SET
                slug = EXCLUDED.slug,
                title = EXCLUDED.title,
                article_md = EXCLUDED.article_md,
                teaser = EXCLUDED.teaser,
                gemini_model = EXCLUDED.gemini_model,
                generated_at = NOW()
            """,
            (place_id, slug, title, article_md, teaser, GEMINI_MODEL),
        )
    conn.commit()


def generate(name: str, address: str) -> str:
    city = extract_city(address)
    prompt = PROMPT_TEMPLATE.format(name=name, city=city)

    client = genai.Client()
    print(f"  Starte Deep Research für '{name}'...")
    interaction = client.interactions.create(
        input=prompt,
        agent=GEMINI_MODEL,
        background=True,
    )
    print(f"  Task ID: {interaction.id}")

    while True:
        interaction = client.interactions.get(interaction.id)
        print(f"  Status: {interaction.status}")
        if interaction.status == "completed":
            return interaction.outputs[-1].text
        elif interaction.status == "failed":
            raise RuntimeError(f"Deep Research fehlgeschlagen: {interaction.error}")
        time.sleep(15)


def run(name: str = None, place_id: str = None):
    conn = get_connection()
    restaurant = find_restaurant(conn, name=name, place_id=place_id)
    if not restaurant:
        print(f"Restaurant nicht gefunden: {name or place_id}")
        return

    r_name = restaurant["name"]
    r_address = restaurant["address"]
    r_place_id = restaurant["place_id"]
    city = extract_city(r_address)

    print(f"\n→ {r_name} ({city})")

    if article_exists(conn, r_place_id):
        print("  Artikel bereits vorhanden. Überspringe. (--force zum Überschreiben)")
        conn.close()
        return

    article_md = generate(r_name, r_address)
    title = extract_title(article_md)
    teaser = extract_teaser(article_md)
    slug = slugify(r_name, city)

    save_article(conn, r_place_id, slug, title, article_md, teaser)
    print(f"  ✓ Gespeichert: slug='{slug}', is_published=FALSE")
    print(f"  Titel: {title}")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("name", nargs="?", help="Restaurant name (partial match)")
    parser.add_argument("--place_id", help="Exact Google Place ID")
    parser.add_argument("--force", action="store_true", help="Overwrite existing article")
    args = parser.parse_args()

    if not args.name and not args.place_id:
        parser.print_help()
    else:
        run(name=args.name, place_id=args.place_id)
