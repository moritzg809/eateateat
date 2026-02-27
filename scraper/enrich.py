"""
mallorcaeat enricher — Step 2

Calls Gemini 2.0 Flash with Google Maps Grounding for each top restaurant.
Results are cached by Google Place ID — each restaurant is enriched at most once,
regardless of how often the Serper scraper runs.

Usage:
    python enrich.py                     # enrich all unenriched top restaurants
    python enrich.py --limit 10          # test with 10 restaurants
    python enrich.py --dry-run           # show what would be called, no API costs
    python enrich.py --force             # re-enrich even if already cached
    python enrich.py --min-rating 4.7    # stricter quality filter
"""

import argparse
import json
import logging
import os
import re
import time

import psycopg2.extras
import requests

from db import count_today_enrichments, get_connection, set_pipeline_status

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
#MODEL = "gemini-2.0-flash"
MODEL = "gemini-3-flash-preview"
GEMINI_URL = (
    f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL}:generateContent"
    f"?key={GEMINI_API_KEY}"
)

PROMPT_TEMPLATE = """\
Du bist ein ehrlicher Mallorca-Insider mit hohen Ansprüchen — kein Tourismusprospekt.
Schlage auf Google Maps "{name}" ({address}) nach und lies die echten Reviews.

Bewerte für 11 Reiseprofile (1–10, ganze Zahlen):

• Familie      (1=Kinder fehl am Platz, 10=Hochstühle, Spielecke, frühe Küche)
• Date Night   (1=Neonlicht & Plastik, 10=gedimmtes Licht, Weinliste, Intimität)
• Friends Trip (1=zu eng/laut für Gruppen, 10=große Tische, Sharing, gesellig)
• Solo         (1=peinliches Alleinsein, 10=Barhocker, offene Atmosphäre, Buch okay)
• Entspannt    (1=Tisch wird nach 90min gebraucht, 10=dritter Kaffee, kein Stress)
• Party Vibe   (1=Ruhe nach 21 Uhr, 10=DJ, Cocktails, tanzen bis 2)
• Besonderer Anlass (1=Canteen-Feeling, 10=erinnert man sich in 10 Jahren)
• Foodie       (1=Tiefkühlware aufgewärmt, 10=Küchenchef mit Handschrift, Saisonalität)
• Verweilen    (1=Rechnung kommt ungefragt, 10=man bleibt 4 Stunden ohne Druck)
• Geheimtipp   (1=Reisebus wartet draußen, 10=keine Touristen, Locals sitzen hier täglich)
• Dress Code   (1=Badeshorts & Flip-Flops okay, 3=Jeans ist ok, 7=T-Shirt ist fehl am Platz, 10=Hemd/Kleid erwartet, Jeans auffällig)

VERBOTEN — diese Phrasen kennzeichnen schlechtes Schreiben, verwende sie nie:
"Ein Muss", "sehr zu empfehlen", "lohnenswert", "einladend", "gemütlich",
"kulinarisches Erlebnis", "gastronomische Reise", "ideal für", "perfekt für",
"dreht sich alles um",
"mehr als nur", "ein Ort an dem", "wer ... sucht",
Sätze die mit "Es ist...", "Das Restaurant bietet...", "Das Restaurant ist..." beginnen

Wenn du dir nicht sicher bist antworte mit "None" - Das heist aber dass es nicht angezeigt werden wird

Für die drei Textfelder gilt:

summary_de — Genau 2 Sätze, strikt nach dieser Struktur:
  Satz 1: Konzept + 1-2 harte Fakten (Küchenchef, Stil, Lage, Preisklasse, Besonderheit).
          Beispiel: "Küchenchef Andreu Genestra kocht hyperregionale Mallorquiner Küche
          in einer alten Finca bei Capdepera — Degustationsmenü ab 85€, Gemüse aus
          eigenem Garten."
  Satz 2: Was passiert dort wirklich — was fällt einem auf, wer sitzt da, was hört/riecht man.
          Beispiel: "An den Tischen sitzen fast nur Mallorquiner, die Frau vom Chef
          erklärt jeden Gang auf Katalanisch, das Brot kommt warm aus dem Holzofen."

must_order — 1-2 konkrete Gerichte/Getränke mit vollem Namen, keine Oberbegriffe.
  Beispiel GUT: "Sobrassada amb mel (auf warmem Pan de cristal), dazu ein Glas
  José L. Ferrer Blanc de Blancs"
  Beispiel SCHLECHT: "Die Tapas" / "Die Hausspezialitäten" / "das Fleisch"

vibe — 1 Satz: Licht + Lautstärke + wer sitzt da + Uhrzeit. Keine Wertungen.
  Beispiel GUT: "Enge Holztische, Neonröhren, fast nur Einheimische, voll ab 21:30,
  man teilt sich die Bank mit Fremden."
  Beispiel SCHLECHT: "Entspannte Atmosphäre mit warmem Licht."

Antworte AUSSCHLIESSLICH mit diesem JSON (kein Markdown, kein Text davor/danach):
{{
  "family":    <int>,
  "date":      <int>,
  "friends":   <int>,
  "solo":      <int>,
  "relaxed":   <int>,
  "party":     <int>,
  "special":   <int>,
  "foodie":    <int>,
  "lingering":  <int>,
  "unique":     <int>,
  "dresscode":  <int>,
  "summary_de": "<2 Sätze, konkret, nur für dieses Restaurant wahr>",
  "must_order": "<1-2 echte Gerichte/Getränke mit Namen>",
  "vibe":       "<1 Satz: Licht, Lautstärke, Gäste, Uhrzeit>"
}}"""

_SESSION = requests.Session()


# ---------------------------------------------------------------------------
# Gemini API
# ---------------------------------------------------------------------------

def _extract_json(text: str) -> dict:
    """Strip optional markdown code fences and parse JSON."""
    text = re.sub(r"```(?:json)?\s*", "", text).strip().rstrip("`").strip()
    return json.loads(text)


def call_gemini(name: str, address: str, lat=None, lng=None, retries: int = 3) -> tuple[dict, dict]:
    prompt = PROMPT_TEMPLATE.format(name=name, address=address or "Mallorca, Spanien")
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "tools": [{"googleMaps": {}}],
        "generationConfig": {"temperature": 0.3},
    }
    if lat is not None and lng is not None:
        payload["toolConfig"] = {
            "retrievalConfig": {
                "latLng": {"latitude": float(lat), "longitude": float(lng)}
            }
        }

    for attempt in range(1, retries + 1):
        try:
            resp = _SESSION.post(GEMINI_URL, json=payload, timeout=60)
            resp.raise_for_status()
            raw = resp.json()
            text = raw["candidates"][0]["content"]["parts"][0]["text"]
            parsed = _extract_json(text)
            return parsed, raw  # (parsed_dict, full_api_response)
        except requests.HTTPError:
            if resp.status_code == 429 or attempt < retries:
                wait = 2 ** attempt
                logger.warning(
                    "HTTP %s – retry in %ss (%d/%d)",
                    resp.status_code, wait, attempt, retries,
                )
                time.sleep(wait)
            else:
                raise
        except (json.JSONDecodeError, KeyError, IndexError) as e:
            logger.warning("Parse error attempt %d/%d: %s", attempt, retries, e)
            if attempt == retries:
                raise
            time.sleep(2)


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def fetch_pending(
    conn, min_rating: float, min_reviews: int, limit: int | None, force: bool
) -> list:
    if force:
        join = ""
        where_cache = ""
    else:
        join = "LEFT JOIN gemini_enrichments e ON e.place_id = r.place_id"
        where_cache = "AND e.place_id IS NULL"

    sql = f"""
        SELECT r.id, r.place_id, r.name, r.address, r.latitude, r.longitude
        FROM   restaurants r
        {join}
        WHERE  r.rating       >= %(min_rating)s
          AND  r.rating_count >= %(min_reviews)s
          {where_cache}
        ORDER BY r.rating DESC, r.rating_count DESC
        {"LIMIT %(limit)s" if limit else ""}
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"min_rating": min_rating, "min_reviews": min_reviews, "limit": limit})
        return cur.fetchall()


def save_enrichment(conn, place_id: str, data: dict, raw_response: dict | None = None):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO gemini_enrichments (
                place_id,
                family_score, date_score,   friends_score, solo_score,
                relaxed_score, party_score, special_score, foodie_score,
                lingering_score, unique_score, dresscode_score,
                summary_de, must_order, vibe, gemini_model, raw_response
            ) VALUES (
                %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s, %s
            )
            ON CONFLICT (place_id) DO UPDATE SET
                family_score    = EXCLUDED.family_score,
                date_score      = EXCLUDED.date_score,
                friends_score   = EXCLUDED.friends_score,
                solo_score      = EXCLUDED.solo_score,
                relaxed_score   = EXCLUDED.relaxed_score,
                party_score     = EXCLUDED.party_score,
                special_score   = EXCLUDED.special_score,
                foodie_score    = EXCLUDED.foodie_score,
                lingering_score = EXCLUDED.lingering_score,
                unique_score    = EXCLUDED.unique_score,
                dresscode_score = EXCLUDED.dresscode_score,
                summary_de      = EXCLUDED.summary_de,
                must_order      = EXCLUDED.must_order,
                vibe            = EXCLUDED.vibe,
                gemini_model    = EXCLUDED.gemini_model,
                raw_response    = EXCLUDED.raw_response,
                enriched_at     = NOW()
            """,
            (
                place_id,
                data.get("family"),   data.get("date"),     data.get("friends"), data.get("solo"),
                data.get("relaxed"),  data.get("party"),    data.get("special"), data.get("foodie"),
                data.get("lingering"), data.get("unique"),  data.get("dresscode"),
                data.get("summary_de"), data.get("must_order"), data.get("vibe"),
                MODEL,
                psycopg2.extras.Json(raw_response) if raw_response else None,
            ),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(limit=None, min_rating=4.5, min_reviews=100, dry_run=False, force=False,
        daily_limit=500):
    conn = get_connection()

    # --- Daily cap check ---
    today_count = count_today_enrichments(conn)
    remaining = daily_limit - today_count
    if remaining <= 0:
        logger.info("=" * 60)
        logger.info("Daily enrichment limit reached (%d/%d) — skipping.", today_count, daily_limit)
        logger.info("=" * 60)
        conn.close()
        return

    # Apply daily cap to limit
    effective_limit = limit
    if limit is None or limit > remaining:
        effective_limit = remaining

    rows = fetch_pending(conn, min_rating, min_reviews, effective_limit, force)
    total = len(rows)

    logger.info("=" * 60)
    logger.info("mallorcaeat enricher — Gemini Maps Grounding")
    logger.info("  model        : %s", MODEL)
    logger.info("  pending      : %d", total)
    logger.info("  today so far : %d / %d daily limit", today_count, daily_limit)
    logger.info("  min rating   : %.1f   min reviews: %d", min_rating, min_reviews)
    logger.info("  cache key    : place_id (pay once per restaurant)")
    if dry_run:
        logger.info("  MODE         : DRY RUN (no API calls, no costs)")
    if force:
        logger.info("  MODE         : FORCE (re-enriching cached entries)")
    logger.info("=" * 60)

    stats = {"ok": 0, "errors": 0}

    for i, (rid, place_id, name, address, lat, lng) in enumerate(rows, 1):
        prefix = f"[{i:>3}/{total}]"

        if dry_run:
            logger.info("%s WOULD ENRICH  %s", prefix, name)
            continue

        logger.info("%s %s", prefix, name)
        try:
            data, raw_response = call_gemini(name, address, lat, lng)
            save_enrichment(conn, place_id, data, raw_response)
            set_pipeline_status(conn, place_id, "enriched")
            stats["ok"] += 1
            logger.info("         -> ✓  %s", data.get("vibe", "")[:90])
            time.sleep(0.3)  # gentle pacing
        except Exception as exc:
            logger.error("         -> ✗  %s", exc)
            stats["errors"] += 1

    conn.close()
    logger.info("=" * 60)
    logger.info("Done.  OK: %d | Errors: %d", stats["ok"], stats["errors"])
    logger.info("=" * 60)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Enrich restaurants via Gemini Maps Grounding")
    ap.add_argument("--limit",       type=int,   default=None,  help="max restaurants to process")
    ap.add_argument("--min-rating",  type=float, default=4.5,   help="minimum Google rating")
    ap.add_argument("--min-reviews", type=int,   default=100,   help="minimum review count")
    ap.add_argument("--dry-run",     action="store_true",       help="no API calls, just show what would run")
    ap.add_argument("--force",       action="store_true",       help="re-enrich even if place_id already cached")
    ap.add_argument("--daily-limit", type=int,   default=500,   help="max enrichments per calendar day (default 500)")
    args = ap.parse_args()

    run(
        limit=args.limit,
        min_rating=args.min_rating,
        min_reviews=args.min_reviews,
        dry_run=args.dry_run,
        force=args.force,
        daily_limit=args.daily_limit,
    )
