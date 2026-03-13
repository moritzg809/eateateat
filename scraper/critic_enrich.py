"""
mallorcaeat critic enricher — backfill-only script

Adds critic-style quality scores, audience dimensions, price estimate, and cuisine
classification to restaurants that already have occasion scores (family, date, …)
but were enriched before migration 004 added the new fields.

For NEW restaurants going forward, these fields are populated automatically by the
extended enrich.py prompt — no need to run this script.

Usage:
    python critic_enrich.py                  # process up to 500 restaurants (daily cap shared)
    python critic_enrich.py --backfill       # process ALL pending, ignore daily cap
    python critic_enrich.py --limit 50       # process at most 50
    python critic_enrich.py --dry-run        # preview without API calls
"""

import argparse
import json
import logging
import os
import re
import time

import psycopg2.extras
from google import genai
from google.genai import types

from db import count_today_enrichments, get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

MODEL  = "gemini-2.5-flash"
client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])

BACKFILL_PROMPT = """\
Du bist ein professioneller Restauranttester für Mallorca.

Restaurant: {name} | {address}
{website_section}\
Schlage "{name}" ({address}) auf Google Maps nach und lies die echten Reviews.
Nutze BEIDE Quellen — die Website verrät Konzept und Anspruch; Google Maps zeigt was Gäste wirklich erleben.

Bewerte ausschließlich die folgenden Dimensionen (Occasion-Scores sind bereits vorhanden):

RESTAURANTKRITIK (1–10, ganze Zahlen):
• cuisine:      Kochqualität — Zutaten, Technik, Geschmack, Konsistenz (Gewicht 45 %)
• service:      Professionalität, Freundlichkeit, Timing (25 %)
• value:        Preis-Leistungs-Verhältnis (20 %)
• ambiance:     Einrichtung, Atmosphäre, Komfort, Sauberkeit (10 %)
• critic_score: Gesamtnote = cuisine×0.45 + service×0.25 + value×0.20 + ambiance×0.10 (gerundet)

MALLORCA-KONTEXT (1–10):
• outdoor:  Terrasse / Außenbereich (1=keiner, 5=einfache Terrasse, 10=traumhafte Outdoor-Fläche)
• view:     Aussicht (1=keine, 5=schöner Blick, 10=spektakulärer Meerblick oder Sonnenuntergang)

ZIELGRUPPE — intern, nicht anzeigen (1–10):
• scene:     Gesehen-werden-Faktor (10=reine Scene/Instagram-Restaurant, 1=unauffällig)
• local:     Einheimische vs. Touristen (10=fast nur Locals, 1=reine Touristenfalle)
• warmth:    Herzlichkeit (10=jeder willkommen, 1=einschüchternd exklusiv)
• substance: Qualität vor Image (10=pure Substanz, 1=Style über Inhalt)
• audience_type: Eine Kategorie: "scene"|"gourmet"|"local"|"family"|"tourist"|"business"|"mixed"

PREIS — intern:
• avg_price_pp: Durchschnittlicher Preis pro Person in Euro als ganze Zahl (nur Essen, ohne Getränke)

KÜCHE — intern, für künftige Filter:
• cuisine_type: Küchenbezeichnung als kurzer Text (z.B. "Mallorquinisch", "Modern Mediterranean", "Tapas")
• cuisine_tags: Die 5 charakteristischsten Schlagworte zu Gerichten/Zutaten/Getränken als Array

Antworte AUSSCHLIESSLICH mit diesem JSON (kein Markdown, kein Text davor/danach):
{{
  "cuisine":      <int>,
  "service":      <int>,
  "value":        <int>,
  "ambiance":     <int>,
  "critic_score": <int>,
  "outdoor":      <int>,
  "view":         <int>,
  "scene":        <int>,
  "local":        <int>,
  "warmth":       <int>,
  "substance":    <int>,
  "audience_type": "<scene|gourmet|local|family|tourist|business|mixed>",
  "avg_price_pp": <int>,
  "cuisine_type": "<z.B. Mallorquinisch>",
  "cuisine_tags": ["<tag1>", "<tag2>", "<tag3>", "<tag4>", "<tag5>"]
}}
Falls keine ausreichenden Daten vorhanden: null
"""


# ---------------------------------------------------------------------------
# Gemini API
# ---------------------------------------------------------------------------

class ModelUncertainError(Exception):
    """Model explicitly has no data for this restaurant."""


def _extract_json(text: str | None) -> dict:
    if not text or text.strip().lower() in ("none", "null", ""):
        raise ModelUncertainError(f"Model returned no data: {text!r}")
    text = re.sub(r"```(?:json)?\s*", "", text).strip().rstrip("`").strip()
    match = re.search(r"\{[\s\S]*\}", text)
    if match:
        text = match.group(0)
    return json.loads(text)


def call_gemini(name: str, address: str, website: str | None = None,
                lat=None, lng=None, retries: int = 3) -> dict:
    website_section = f"Lies zuerst die offizielle Website: {website}\n" if website else ""

    prompt = BACKFILL_PROMPT.format(
        name=name,
        address=address or "Mallorca, Spanien",
        website_section=website_section,
    )

    tools = [types.Tool(google_maps=types.GoogleMaps())]
    if website:
        tools.append(types.Tool(url_context=types.UrlContext()))

    config_kwargs: dict = {"tools": tools, "temperature": 0.3}
    if lat is not None and lng is not None:
        config_kwargs["tool_config"] = types.ToolConfig(
            retrieval_config=types.RetrievalConfig(
                lat_lng=types.LatLng(latitude=float(lat), longitude=float(lng))
            )
        )

    for attempt in range(1, retries + 1):
        try:
            response = client.models.generate_content(
                model=MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(**config_kwargs),
            )

            text = None
            try:
                text = response.text
            except Exception:
                for candidate in (response.candidates or []):
                    for part in (candidate.content.parts or []):
                        if hasattr(part, "text") and part.text:
                            text = (text or "") + part.text

            return _extract_json(text)

        except ModelUncertainError:
            raise
        except (json.JSONDecodeError, KeyError, IndexError) as e:
            logger.warning("Parse error attempt %d/%d: %s", attempt, retries, e)
            if attempt == retries:
                raise
            time.sleep(2)
        except Exception as exc:
            if attempt < retries:
                wait = 2 ** attempt
                logger.warning("API error – retry in %ds (%d/%d): %s", wait, attempt, retries, exc)
                time.sleep(wait)
            else:
                raise


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def fetch_pending(conn, limit: int | None) -> list:
    """Return complete restaurants that still need the critic fields (critic_score IS NULL)."""
    sql = """
        SELECT r.id, r.place_id, r.name, r.address, r.latitude, r.longitude, r.website
        FROM   restaurants r
        JOIN   gemini_enrichments e ON e.place_id = r.place_id
        WHERE  r.pipeline_status = 'complete'
          AND  r.is_active = TRUE
          AND  e.critic_score IS NULL
          AND  e.family_score IS NOT NULL   -- only rows that have occasion scores
        ORDER BY r.rating DESC, r.rating_count DESC
        {limit_clause}
    """.format(limit_clause=f"LIMIT {limit}" if limit else "")
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def save_critic_fields(conn, place_id: str, data: dict):
    """Update only the critic fields — leave all existing occasion scores untouched."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE gemini_enrichments SET
                cuisine_score   = %(cuisine)s,
                service_score   = %(service)s,
                value_score     = %(value)s,
                ambiance_score  = %(ambiance)s,
                critic_score    = %(critic_score)s,
                outdoor_score   = %(outdoor)s,
                view_score      = %(view)s,
                scene_score     = %(scene)s,
                local_score     = %(local)s,
                warmth_score    = %(warmth)s,
                substance_score = %(substance)s,
                audience_type   = %(audience_type)s,
                avg_price_pp    = %(avg_price_pp)s,
                cuisine_type    = %(cuisine_type)s,
                cuisine_tags    = %(cuisine_tags)s,
                enriched_at     = NOW()
            WHERE place_id = %(place_id)s
            """,
            {
                "place_id":     place_id,
                "cuisine":      data.get("cuisine"),
                "service":      data.get("service"),
                "value":        data.get("value"),
                "ambiance":     data.get("ambiance"),
                "critic_score": data.get("critic_score"),
                "outdoor":      data.get("outdoor"),
                "view":         data.get("view"),
                "scene":        data.get("scene"),
                "local":        data.get("local"),
                "warmth":       data.get("warmth"),
                "substance":    data.get("substance"),
                "audience_type":data.get("audience_type"),
                "avg_price_pp": data.get("avg_price_pp"),
                "cuisine_type": data.get("cuisine_type"),
                "cuisine_tags": data.get("cuisine_tags") or None,
            },
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(limit=None, dry_run=False, backfill=False, daily_limit=500):
    conn = get_connection()

    if not backfill:
        today_count = count_today_enrichments(conn)
        remaining   = daily_limit - today_count
        if remaining <= 0:
            logger.info("=" * 60)
            logger.info("Daily enrichment limit reached (%d/%d) — skipping.", today_count, daily_limit)
            logger.info("Use --backfill to ignore the daily cap.")
            logger.info("=" * 60)
            conn.close()
            return
        effective_limit = limit if (limit is not None and limit <= remaining) else remaining
    else:
        effective_limit = limit  # None = no cap

    rows  = fetch_pending(conn, effective_limit)
    total = len(rows)

    logger.info("=" * 60)
    logger.info("mallorcaeat critic enricher — backfill")
    logger.info("  model        : %s", MODEL)
    logger.info("  pending      : %d", total)
    if backfill:
        logger.info("  MODE         : BACKFILL (daily cap ignored)")
    if dry_run:
        logger.info("  MODE         : DRY RUN (no API calls, no costs)")
    logger.info("=" * 60)

    stats = {"ok": 0, "skipped": 0, "errors": 0}

    for i, (rid, place_id, name, address, lat, lng, website) in enumerate(rows, 1):
        prefix = f"[{i:>4}/{total}]"

        if dry_run:
            logger.info("%s WOULD ENRICH  %s", prefix, name)
            continue

        logger.info("%s %s  %s", prefix, name, f"🌐 {website}" if website else "")
        try:
            data = call_gemini(name, address, website, lat, lng)
            save_critic_fields(conn, place_id, data)
            stats["ok"] += 1
            logger.info("         -> ✓  critic=%s outdoor=%s view=%s cuisine_type=%s",
                        data.get("critic_score"), data.get("outdoor"),
                        data.get("view"), data.get("cuisine_type", ""))
            time.sleep(0.3)
        except ModelUncertainError as exc:
            logger.warning("         -> ⚠  Modell unsicher, übersprungen: %s", exc)
            stats["skipped"] += 1
        except Exception as exc:
            logger.error("         -> ✗  %s", exc)
            stats["errors"] += 1

    conn.close()
    logger.info("=" * 60)
    logger.info("Done.  OK: %d | Skipped: %d | Errors: %d",
                stats["ok"], stats["skipped"], stats["errors"])
    logger.info("=" * 60)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Backfill critic enrichment fields via Gemini")
    ap.add_argument("--limit",       type=int,   default=None,  help="max restaurants to process")
    ap.add_argument("--dry-run",     action="store_true",       help="no API calls, just show what would run")
    ap.add_argument("--backfill",    action="store_true",       help="ignore daily cap — for one-time backfill of existing restaurants")
    ap.add_argument("--daily-limit", type=int,   default=500,   help="max enrichments per calendar day (default 500, ignored with --backfill)")
    args = ap.parse_args()

    run(
        limit=args.limit,
        dry_run=args.dry_run,
        backfill=args.backfill,
        daily_limit=args.daily_limit,
    )
