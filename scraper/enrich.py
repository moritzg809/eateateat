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

MODEL  = "gemini-3-flash-preview"
client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])

PROMPT_TEMPLATE = """\
Du bist Restaurantkritiker für ein Mallorca-Insider-Magazin. Du schreibst für Menschen die wirklich \
gutes Essen suchen und Touristenfallen hassen. Kein Marketing. Keine Werbesprache. Nur was wirklich stimmt.

SCHRITT 1 — Quellen lesen:
{website_section}\
Schlage "{name}" ({address}) auf Google Maps nach. Lies die Reviews — besonders die kritischen.

Notiere intern (NICHT ausgeben):
• Küchenchef / Gründer — Name, falls bekannt
• Gründungsjahr oder besondere Geschichte, falls erwähnt
• 2-3 Gerichte mit echtem Namen aus Karte oder Reviews
• Was Gäste konkret loben — nicht "gutes Essen" sondern welches Gericht, warum
• Was Gäste konkret kritisieren
• Wer sitzt dort wirklich: Locals 40+? Touristen? Paare? Familien mit Kindern?
• Zu welcher Uhrzeit ist es voll / leer?

Wenn du zu wenig Informationen findest um spezifisch zu sein → antworte nur mit dem Wort "None".

SCHRITT 2 — Drei Texte schreiben:

━━━ ABSOLUT VERBOTEN ━━━
Folgende Phrasen und Satzmuster sind verboten — sie bedeuten generisches, wertloses Schreiben.
Enthält einer deiner Texte eine dieser Phrasen, schreib ihn komplett neu.

Verbotene Wörter/Phrasen:
"gemütlich", "einladend", "lohnenswert", "Ein Muss", "sehr zu empfehlen",
"kulinarisches Erlebnis", "gastronomische Reise", "ideal für", "perfekt für",
"helles Tageslicht", "lebhafte Gespräche", "lebhafte Geräuschkulisse",
"warme Atmosphäre", "tolle Aussicht", "Mischung aus Einheimischen und Touristen",
"dreht sich alles um", "mehr als nur", "ein Ort an dem", "wer ... sucht",
"angeregten Gesprächen", "anregende Gespräche"

Verbotene Satzmuster (gilt auch für ähnliche Formulierungen):
"Es ist...", "Das Restaurant bietet...", "Das Restaurant ist...",
"[Name des Restaurants] bietet...", "[Name des Restaurants] ist...",
"Hier findet man...", "Hier trifft man...",
"... die für ihre/seine ... bekannt ist/sind ...",
"... bekannt für ihre/seine ...",
"... eine Kombination aus ... und ...",
"... verbindet ... mit ...",
"... setzt auf ...", "... überzeugt mit ...",
"... die ... vereint", "... wo ... auf ... trifft"

Verbotene Satzstruktur — Füllrelativsätze:
Jeder Nebensatz der mit "die/der/das/wo ... ist/sind/wird" endet und nur beschreibt
was das Restaurant *hat*, statt was man *erlebt*, ist verboten.
NEIN: "... Gerichte, die auf saisonalen Produkten basieren."
NEIN: "... eine Küche, die mediterrane und japanische Einflüsse vereint."
JA: konkrete Beobachtung, konkretes Detail, kein erklärendes Relativpronomen.
━━━━━━━━━━━━━━━━━━━━━━━━

── summary_de ──
2-4 Sätze. Beginne mit dem was dieses Restaurant von allen anderen unterscheidet —
Küchenchef mit Namen, Gründungsgeschichte, ungewöhnliches Konzept, konkrete Lage.
Nutze echte Namen, echte Gerichte, echte Details aus den Quellen.
Jeder Satz muss für genau dieses Restaurant wahr sein — kein Satz darf für 50 andere auch stimmen.

NEIN: "Can Moranta ist eine traditionelle Bäckerei in Consell, die für ihre authentischen
  mallorquinischen Backwaren bekannt ist und von Einheimischen hochgeschätzt wird."
  → Könnte für jede Bäckerei auf der Insel stehen. Kein einziges spezifisches Detail.

JA:  "Can Moranta backt seit drei Generationen in Consell — die Leinentaschen der Stammkunden
  stehen morgens um halb acht schon vor der Tür. Coca de Patata nach Familienrezept,
  der Ofen läuft seit 5 Uhr, mittags ist alles weg."

NEIN: "De Tokio a Lima verbindet mediterrane, japanische und peruanische Einflüsse zu einer
  avantgardistischen Küche, die auf frischen, saisonalen Produkten basiert."
  → Marketingsprache, null Information über das echte Restaurant.

JA:  "German de Bernardi kocht Nikkei in Valldemossa — Black Cod mit Misoglasur seit Eröffnung
  auf der Karte, Pisco Sour wird an fast jedem Tisch bestellt. Reservierung nötig,
  Fensterplatz lohnt den Aufpreis beim Buchen."

── must_order ──
1-2 Gerichte oder Getränke mit vollem Namen + ein konkreter Satz warum (Textur, Technik,
was Gäste sagen). Keine Oberbegriffe, keine Listen ohne Kontext.

NEIN: "Pulpo a la Gallega, Croquetas de Jamón"
  → Zwei Namen ohne Kontext, könnte jede Tapas-Bar sein.

NEIN: "Die Ensaimada" / "Das Fleisch" / "Die Tapas" / "Die Hausspezialitäten"
  → Oberbegriffe, wertlos.

JA:  "Sobrassada amb mel auf warmem Pan de cristal — die karamellisierte Oberfläche,
  Süße trifft Schärfe, fast jeder Tisch bestellt es laut Reviews."

JA:  "Black Cod mit Misoglasur (laut Karte 24h mariniert) — Reviewers nennen es
  durchgehend das beste Gericht, Textur wie Butter."

── vibe ──
2-3 Sätze. Beschreib den Moment wenn man reinkommt: konkretes Licht, konkrete Geräusche,
wer genau da sitzt, wann. Keine Wertungen. Keine generischen Beobachtungen.
Schreib nur was für exakt dieses Restaurant stimmt — nicht für hundert andere.

NEIN: "Helles Licht, lebhafte Gespräche, gemischtes Publikum, ab 20 Uhr gut besucht."
  → Eine Aufzählung, keine Beschreibung. Gilt für jede Bar der Welt.

NEIN: "Gedämpftes Licht fällt auf elegant gedeckte Tische, während leise Gespräche
  die Geräuschkulisse bilden und Paare den Sonnenuntergang genießen."
  → Generisches Restaurantklischee, komplett austauschbar.

JA:  "Mittagsservice: Pickup-Schlange bis zur Tür, kein Sitzplatz, Backgeruch
  durch die ganze Carrer. Oma am Tresen kennt jeden beim Namen."

JA:  "Abends ab 21 Uhr kommen Paare mit Reservierung, Kerzenlicht auf Naturstein,
  die Köche durch die offene Küche sichtbar. Der Tisch nebenan spricht Mallorquinisch."

⚠️  SELBST-CHECK: Lies deine drei Texte bevor du antwortest nochmal durch.
Enthält einer eine verbotene Phrase oder einen verbotenen Satzbeginn? → Schreib ihn neu.
Könnte ein Satz — mit minimalem Austausch des Namens — für ein anderes Restaurant in dieser Stadt genauso stehen? → Schreib ihn komplett neu.
Gilt das besonders für: Sätze mit "bekannt für", "verbindet X mit Y", "setzt auf", Relativsätze die nur beschreiben was das Restaurant *hat*. → Alle raus.

SCHRITT 3 — Zahlen vergeben:

Reiseprofile (1–10, ganze Zahlen):
• Familie      (1=Kinder fehl am Platz, 10=Hochstühle, Spielecke, frühe Küche)
• Date Night   (1=Neonlicht & Plastik, 10=gedimmtes Licht, Weinliste, Intimität)
• Friends Trip (1=zu eng/laut für Gruppen, 10=große Tische, Sharing, gesellig)
• Solo         (1=peinliches Alleinsein, 10=Barhocker, offene Atmosphäre, Buch okay)
• Entspannt    (1=Tisch wird nach 90min gebraucht, 10=dritter Kaffee, kein Stress)
• Party Vibe   (1=Ruhe nach 21 Uhr, 10=DJ, Cocktails, tanzen bis 2)
• Besonderer Anlass (1=Canteen-Feeling, 10=erinnert man sich in 10 Jahren)
• Foodie       (1=Tiefkühlware aufgewärmt, 10=Küchenchef mit Handschrift, Saisonalität)
• Verweilen    (1=Rechnung kommt ungefragt, 10=man bleibt 4 Stunden ohne Druck)
• Geheimtipp   (1=Reisebus wartet draußen, 10=keine Touristen, Locals täglich)
• Dress Code   (1=Badeshorts okay, 3=Jeans ist ok, 7=T-Shirt fehl am Platz, 10=Hemd/Kleid erwartet)

Restaurantkritik (1–10):
• cuisine:      Kochqualität — Zutaten, Technik, Geschmack, Konsistenz (45 %)
• service:      Professionalität, Freundlichkeit, Timing (25 %)
• value:        Preis-Leistungs-Verhältnis (20 %)
• ambiance:     Einrichtung, Atmosphäre, Komfort, Sauberkeit (10 %)
• critic_score: cuisine×0.45 + service×0.25 + value×0.20 + ambiance×0.10 (gerundet)

Mallorca-Kontext (1–10):
• outdoor:  Außenbereich (1=keiner, 5=einfache Terrasse, 10=traumhafte Outdoor-Fläche)
• view:     Aussicht (1=keine, 5=schöner Blick, 10=spektakulärer Meerblick)

Zielgruppe (1–10):
• scene:     Gesehen-werden-Faktor (10=reines Instagram-Restaurant, 1=unauffällig)
• local:     Einheimische vs. Touristen (10=fast nur Locals, 1=reine Touristenfalle)
• warmth:    Herzlichkeit (10=jeder willkommen, 1=einschüchternd exklusiv)
• substance: Qualität vor Image (10=pure Substanz, 1=Style über Inhalt)
• audience_type: "scene"|"gourmet"|"local"|"family"|"tourist"|"business"|"mixed"

Preis:
• avg_price_pp: Durchschnitt pro Person in Euro, nur Essen ohne Getränke

Küche:
• cuisine_type: auf DEUTSCH, max. 3 Wörter ("Mallorquinisch", "Modern-Mediterran", "Tapas-Bar", "Café & Brunch", "Weinbar")
• cuisine_tags: 5 charakteristischste Schlagworte zu Gerichten/Zutaten/Getränken

Optik & Stil — ehrlich, auch Schwächen nennen:
• interior_tags: 5 ehrlichste Schlagworte zu Einrichtung/Atmosphäre
  Positiv: "Gewölbekeller", "Rooftop-Terrasse", "Rustikale Finca", "Marmor & Messing", "Kerzenschein"
  Negativ: "Plastikstühle", "Neonlicht", "Touristenfalle-Deko", "Beengt & laut", "Ikea-Feeling"
• food_tags: 5 ehrlichste Schlagworte zur Speisen-Optik
  Positiv: "Fine-Dining-Plating", "Farbenfroh & frisch", "Üppige Portionen", "Handgemacht-Optik"
  Negativ: "Lieblos angerichtet", "Fertigware-Optik", "Zu kleine Portionen", "Touristenportion"

Antworte AUSSCHLIESSLICH mit diesem JSON (kein Markdown, kein Text davor/danach):
{{
  "summary_de":   "<2-4 Sätze, spezifisch, kein generischer Satz>",
  "must_order":   "<1-2 echte Gerichte mit Namen + warum>",
  "vibe":         "<2-3 Sätze: konkretes Licht, konkrete Gäste, konkrete Uhrzeit>",
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
  "cuisine_tags":  ["<tag1>", "<tag2>", "<tag3>", "<tag4>", "<tag5>"],
  "interior_tags": ["<tag1>", "<tag2>", "<tag3>", "<tag4>", "<tag5>"],
  "food_tags":     ["<tag1>", "<tag2>", "<tag3>", "<tag4>", "<tag5>"]
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

    # Build tools — url_context and google_maps cannot be combined in one request;
    # prefer url_context when a website is available, otherwise fall back to google_maps
    if website:
        tools = [types.Tool(url_context=types.UrlContext())]
    else:
        tools = [types.Tool(google_maps=types.GoogleMaps())]

    # Build config — lat/lng is optional
    config_kwargs: dict = {
        "tools":       tools,
        "temperature": 1.0,  # required for thinking mode
        "thinking_config": types.ThinkingConfig(thinking_level="low"),
        "automatic_function_calling": types.AutomaticFunctionCallingConfig(
            maximum_remote_calls=15
        ),
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


def save_enrichment(conn, place_id: str, data: dict, raw_response: dict | None = None,
                    maps_used: bool = True):
    """Persist a Gemini enrichment result.

    maps_used=False when the call used url_context instead of google_maps — those
    enrichments don't consume Google Maps quota and are excluded from the daily cap.
    """
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
                interior_tags,  food_tags,
                summary_de, must_order, vibe, gemini_model, raw_response, maps_used
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
                %s, %s,
                %s, %s, %s, %s, %s, %s
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
                interior_tags   = EXCLUDED.interior_tags,
                food_tags       = EXCLUDED.food_tags,
                summary_de      = EXCLUDED.summary_de,
                must_order      = EXCLUDED.must_order,
                vibe            = EXCLUDED.vibe,
                gemini_model    = EXCLUDED.gemini_model,
                raw_response    = EXCLUDED.raw_response,
                maps_used       = EXCLUDED.maps_used,
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
                data.get("interior_tags") or None, data.get("food_tags") or None,
                data.get("summary_de"), data.get("must_order"), data.get("vibe"),
                MODEL,
                psycopg2.extras.Json(raw_response) if raw_response else None,
                maps_used,
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
    logger.info("mallorcaeat enricher — Gemini Maps Grounding + Thinking")
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

        # url_context is used when a website is available; google_maps otherwise.
        # Only google_maps calls count towards the 500/day Google Maps quota cap.
        used_maps = not bool(website)
        logger.info("%s %s  %s", prefix, name,
                    f"🌐 {website}" if website else "🗺 google_maps")
        try:
            data, raw_response = call_gemini(name, address, lat, lng, website)
            save_enrichment(conn, place_id, data, raw_response, maps_used=used_maps)
            set_pipeline_status(conn, place_id, "enriched")
            stats["ok"] += 1
            logger.info("         -> ✓  %s", data.get("vibe", "")[:90])
            time.sleep(0.3)  # gentle pacing
        except ModelUncertainError as exc:
            logger.warning("         -> ⚠  Modell unsicher, übersprungen: %s", exc)
            conn.rollback()  # reset any aborted transaction before saving fallback row
            # Save a null row so this restaurant no longer appears as "pending"
            # and doesn't permanently block gem_qualify. Re-run with --force to retry.
            save_enrichment(conn, place_id, {}, {"model": MODEL, "text": None, "uncertain": True},
                            maps_used=used_maps)
            stats["skipped"] = stats.get("skipped", 0) + 1
        except Exception as exc:
            logger.error("         -> ✗  %s", exc)
            conn.rollback()  # reset aborted transaction so the next restaurant can proceed
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
