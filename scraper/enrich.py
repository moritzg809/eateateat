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
from google import genai
from google.genai import types

from db import count_today_enrichments, get_connection, set_pipeline_status

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

MODEL  = "gemini-2.5-flash"
client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])

PROMPT_TEMPLATE = """\
Du bist ein ehrlicher Mallorca-Insider mit hohen Ansprüchen — kein Tourismusprospekt.

SCHRITT 1 — Primärquellen lesen:
{website_section}\
Schlage dann "{name}" ({address}) auf Google Maps nach und lies die echten Reviews.
Nutze BEIDE Quellen — die Website verrät Konzept, Geschichte und Anspruch des Restaurants;
Google Maps zeigt was Gäste wirklich erleben.

SCHRITT 2 — Fakten notieren (intern, nicht ausgeben):
Bevor du antwortest, halte für dich fest:
• Küchenchef / Gründer (Name, falls bekannt)
• Gründungsjahr oder Geschichte (falls erwähnt)
• Küchenstil / Konzept in einem Wort
• 2-3 Signaturgerichte mit echtem Namen
• Was Gäste in Reviews besonders erwähnen (positiv UND negativ)
• Wer sitzt dort typischerweise (Locals, Touristen, Alter, Anlass)
• Uhrzeit / Tageszeit wann es voll ist
Diese Fakten MÜSSEN in deinen Antworten auftauchen — sonst ist die Antwort wertlos.

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

RESTAURANTKRITIK — wie ein professioneller Restauranttester (1–10):
• cuisine:      Kochqualität — Zutaten, Technik, Geschmack, Konsistenz (Gewicht 45 %)
• service:      Professionalität, Freundlichkeit, Timing (25 %)
• value:        Preis-Leistungs-Verhältnis (20 %)
• ambiance:     Einrichtung, Atmosphäre, Komfort, Sauberkeit (10 %)
• critic_score: Gesamtnote = cuisine×0.45 + service×0.25 + value×0.20 + ambiance×0.10 (gerundet auf ganze Zahl)

MALLORCA-KONTEXT — für Filter, nicht anzeigen (1–10):
• outdoor:  Terrasse / Außenbereich (1=keiner, 5=einfache Terrasse, 10=traumhafte Outdoor-Fläche)
• view:     Aussicht (1=keine, 5=schöner Blick, 10=spektakulärer Meerblick oder Sonnenuntergang)

ZIELGRUPPE — intern, nicht anzeigen (1–10):
• scene:     Gesehen-werden-Faktor (10=reine Scene/Instagram-Restaurant, 1=unauffällig)
• local:     Einheimische vs. Touristen (10=fast nur Locals, 1=reine Touristenfalle)
• warmth:    Herzlichkeit (10=jeder willkommen, 1=einschüchternd exklusiv)
• substance: Qualität vor Image (10=pure Substanz, 1=Style über Inhalt)
• audience_type: Eine Kategorie: "scene"|"gourmet"|"local"|"family"|"tourist"|"business"|"mixed"

PREIS — intern, nicht anzeigen:
• avg_price_pp: Durchschnittlicher Preis pro Person in Euro als ganze Zahl (nur Essen, ohne Getränke)

KÜCHE — intern, für künftige Filter:
• cuisine_type: Küchenbezeichnung als kurzer Text (z.B. "Mallorquinisch", "Modern Mediterranean", "Tapas", "Japanisch-Peruanisch")
• cuisine_tags: Die 5 charakteristischsten Schlagworte zu Gerichten/Zutaten/Getränken als Array (z.B. ["Tumbet", "Sobrasada", "Pa amb oli", "Ensaimada", "Cava"])

VERBOTEN — diese Phrasen kennzeichnen schlechtes Schreiben, verwende sie nie:
"Ein Muss", "sehr zu empfehlen", "lohnenswert", "einladend", "gemütlich",
"kulinarisches Erlebnis", "gastronomische Reise", "ideal für", "perfekt für",
"dreht sich alles um", "mehr als nur", "ein Ort an dem", "wer ... sucht",
"helles Tageslicht", "lebhafte Gespräche", "lebhafte Geräuschkulisse",
"Mischung aus Einheimischen und Touristen", "warme Atmosphäre", "tolle Aussicht",
Sätze die mit "Es ist...", "Das Restaurant bietet...", "Das Restaurant ist..." beginnen

Wenn du dir nicht sicher bist antworte mit "None" - Das heist aber dass es nicht angezeigt werden wird

Für die drei Textfelder gilt:

summary_de — 2-4 Sätze, keine starre Struktur, aber konkret und spezifisch:
  Beginne mit dem was dieses Restaurant einzigartig macht — Küchenchef, Gründungsgeschichte, Konzept, Lage.
  Wenn Website oder Reviews einen Namen, ein Gründungsjahr, eine Geschichte nennen — nutze sie.
  Danach: was passiert dort wirklich, wer sitzt da, was fällt auf.
  Jeder Satz muss nur für dieses Restaurant wahr sein — kein Satz darf für 50 andere Restaurants genauso gelten.
  Beispiel: "Küchenchef Andreu Genestra kocht hyperregionale Mallorquiner Küche in einer alten Finca bei
  Capdepera — Degustationsmenü ab 85€, Gemüse aus eigenem Garten. An den Tischen sitzen fast nur
  Mallorquiner, die Frau vom Chef erklärt jeden Gang auf Katalanisch, das Brot kommt warm aus dem Holzofen.
  Reservierung Monate im Voraus, trotzdem fühlt sich kein Tisch gehetzt an."

must_order — 1-2 konkrete Gerichte/Getränke mit vollem Namen aus den Reviews oder der Karte, keine Oberbegriffe.
  Beispiel GUT: "Sobrassada amb mel (auf warmem Pan de cristal), dazu ein Glas José L. Ferrer Blanc de Blancs"
  Beispiel SCHLECHT: "Die Tapas" / "Die Hausspezialitäten" / "das Fleisch"

vibe — Beschreibe in 1-3 Sätzen wie es sich dort anfühlt. Kein vorgegebenes Schema — schreib was einen wirklich
  trifft wenn man reinkommt: das Licht, die Geräusche, wer da sitzt, zu welcher Uhrzeit, welche Energie im Raum ist.
  Keine Wertungen ("schön", "toll", "gemütlich"). Keine generischen Beobachtungen.
  Was du schreibst muss für genau dieses Restaurant stimmen — und sich von allen anderen unterscheiden.

Antworte AUSSCHLIESSLICH mit diesem JSON (kein Markdown, kein Text davor/danach):
{{
  "family":       <int>,
  "date":         <int>,
  "friends":      <int>,
  "solo":         <int>,
  "relaxed":      <int>,
  "party":        <int>,
  "special":      <int>,
  "foodie":       <int>,
  "lingering":    <int>,
  "unique":       <int>,
  "dresscode":    <int>,
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
  "cuisine_tags": ["<tag1>", "<tag2>", "<tag3>", "<tag4>", "<tag5>"],
  "summary_de":   "<2 Sätze, konkret, nur für dieses Restaurant wahr>",
  "must_order":   "<1-2 echte Gerichte/Getränke mit Namen>",
  "vibe":         "<1 Satz: Licht, Lautstärke, konkrete Gäste, Uhrzeit>"
}}"""

# ---------------------------------------------------------------------------
# Gemini API (google-genai SDK)
# ---------------------------------------------------------------------------

class ModelUncertainError(Exception):
    """Raised when the model explicitly signals it has no data (responds 'None')."""


def _extract_json(text: str | None) -> dict:
    """Strip optional markdown code fences and parse JSON.

    Raises ModelUncertainError if the model returned 'None' (no data available).
    Raises json.JSONDecodeError if the text cannot be parsed as JSON.
    """
    if not text or text.strip().lower() in ("none", "null", ""):
        raise ModelUncertainError(f"Model returned no data: {text!r}")

    # Strip markdown code fences
    text = re.sub(r"```(?:json)?\s*", "", text).strip().rstrip("`").strip()

    # If there's surrounding prose, try to extract the JSON object
    match = re.search(r"\{[\s\S]*\}", text)
    if match:
        text = match.group(0)

    return json.loads(text)


def call_gemini(name: str, address: str, lat=None, lng=None, website: str | None = None, retries: int = 3) -> tuple[dict, dict]:
    if website:
        website_section = f"Lies zuerst die offizielle Website: {website}\n"
    else:
        website_section = ""

    prompt = PROMPT_TEMPLATE.format(
        name=name,
        address=address or "Mallorca, Spanien",
        website_section=website_section,
    )

    # Build tools — always Google Maps, add URL context if website available
    tools = [types.Tool(google_maps=types.GoogleMaps())]
    if website:
        tools.append(types.Tool(url_context=types.UrlContext()))

    # Build config — lat/lng is optional
    config_kwargs: dict = {
        "tools":       tools,
        "temperature": 0.3,
    }
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

            # response.text can be None when the model only emits tool calls
            text = None
            try:
                text = response.text
            except Exception:
                # Fallback: collect all text parts manually
                for candidate in (response.candidates or []):
                    for part in (candidate.content.parts or []):
                        if hasattr(part, "text") and part.text:
                            text = (text or "") + part.text

            parsed = _extract_json(text)  # raises ModelUncertainError or JSONDecodeError

            # Build serialisable raw dict (grounding sources + text)
            raw: dict = {"model": MODEL, "text": text, "sources": []}
            try:
                gm = response.candidates[0].grounding_metadata
                if gm and gm.grounding_chunks:
                    raw["sources"] = [
                        {"title": c.maps.title, "uri": c.maps.uri}
                        for c in gm.grounding_chunks
                        if c.maps
                    ]
            except (IndexError, AttributeError):
                pass

            return parsed, raw

        except ModelUncertainError as e:
            # Model has no data for this restaurant — skip, don't retry
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
        SELECT r.id, r.place_id, r.name, r.address, r.latitude, r.longitude, r.website
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
                family_score,   date_score,     friends_score,  solo_score,
                relaxed_score,  party_score,    special_score,  foodie_score,
                lingering_score, unique_score,  dresscode_score,
                cuisine_score,  service_score,  value_score,    ambiance_score, critic_score,
                outdoor_score,  view_score,
                scene_score,    local_score,    warmth_score,   substance_score,
                audience_type,  avg_price_pp,
                cuisine_type,   cuisine_tags,
                summary_de, must_order, vibe, gemini_model, raw_response
            ) VALUES (
                %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s,
                %s, %s, %s, %s,
                %s, %s,
                %s, %s,
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
                cuisine_score   = EXCLUDED.cuisine_score,
                service_score   = EXCLUDED.service_score,
                value_score     = EXCLUDED.value_score,
                ambiance_score  = EXCLUDED.ambiance_score,
                critic_score    = EXCLUDED.critic_score,
                outdoor_score   = EXCLUDED.outdoor_score,
                view_score      = EXCLUDED.view_score,
                scene_score     = EXCLUDED.scene_score,
                local_score     = EXCLUDED.local_score,
                warmth_score    = EXCLUDED.warmth_score,
                substance_score = EXCLUDED.substance_score,
                audience_type   = EXCLUDED.audience_type,
                avg_price_pp    = EXCLUDED.avg_price_pp,
                cuisine_type    = EXCLUDED.cuisine_type,
                cuisine_tags    = EXCLUDED.cuisine_tags,
                summary_de      = EXCLUDED.summary_de,
                must_order      = EXCLUDED.must_order,
                vibe            = EXCLUDED.vibe,
                gemini_model    = EXCLUDED.gemini_model,
                raw_response    = EXCLUDED.raw_response,
                enriched_at     = NOW()
            """,
            (
                place_id,
                data.get("family"),    data.get("date"),     data.get("friends"),  data.get("solo"),
                data.get("relaxed"),   data.get("party"),    data.get("special"),  data.get("foodie"),
                data.get("lingering"), data.get("unique"),   data.get("dresscode"),
                data.get("cuisine"),   data.get("service"),  data.get("value"),    data.get("ambiance"), data.get("critic_score"),
                data.get("outdoor"),   data.get("view"),
                data.get("scene"),     data.get("local"),    data.get("warmth"),   data.get("substance"),
                data.get("audience_type"), data.get("avg_price_pp"),
                data.get("cuisine_type"), data.get("cuisine_tags") or None,
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

    for i, (rid, place_id, name, address, lat, lng, website) in enumerate(rows, 1):
        prefix = f"[{i:>3}/{total}]"

        if dry_run:
            logger.info("%s WOULD ENRICH  %s  %s", prefix, name, f"[{website}]" if website else "")
            continue

        logger.info("%s %s  %s", prefix, name, f"🌐 {website}" if website else "")
        try:
            data, raw_response = call_gemini(name, address, lat, lng, website)
            save_enrichment(conn, place_id, data, raw_response)
            set_pipeline_status(conn, place_id, "enriched")
            stats["ok"] += 1
            logger.info("         -> ✓  %s", data.get("vibe", "")[:90])
            time.sleep(0.3)  # gentle pacing
        except ModelUncertainError as exc:
            logger.warning("         -> ⚠  Modell unsicher, übersprungen: %s", exc)
            # Save a null row so this restaurant no longer appears as "pending"
            # and doesn't permanently block gem_qualify. Re-run with --force to retry.
            save_enrichment(conn, place_id, {}, {"model": MODEL, "text": None, "uncertain": True})
            stats["skipped"] = stats.get("skipped", 0) + 1
        except Exception as exc:
            logger.error("         -> ✗  %s", exc)
            stats["errors"] += 1

    conn.close()
    logger.info("=" * 60)
    logger.info("Done.  OK: %d | Skipped: %d | Errors: %d",
                stats["ok"], stats.get("skipped", 0), stats["errors"])
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
