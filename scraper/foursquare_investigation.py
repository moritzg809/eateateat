"""
Foursquare Coverage Investigation
----------------------------------
Samples 100 random restaurants from DB and checks:
  - How many are found on Foursquare (within 100m radius + name query)
  - Jaro-Winkler similarity between DB name and Foursquare name

Usage:
    FSQ_API_KEY=<your_key> DATABASE_URL=... python foursquare_investigation.py

Foursquare API key: https://developer.foursquare.com (free tier: 1000 req/day)
"""

import os
import sys
import time
import statistics
import logging
import psycopg2
import psycopg2.extras
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

FSQ_API_KEY  = os.environ.get("FSQ_API_KEY", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://mallorcaeat:mallorcaeat_dev@localhost:5433/mallorcaeat")
SAMPLE_SIZE  = 100
SEARCH_RADIUS = 100   # metres
REQUEST_DELAY = 0.25  # seconds between calls (stays well within free tier)


# ── Jaro-Winkler ──────────────────────────────────────────────────────────────

def _jaro(s1: str, s2: str) -> float:
    s1, s2 = s1.lower().strip(), s2.lower().strip()
    if s1 == s2:
        return 1.0
    len1, len2 = len(s1), len(s2)
    if len1 == 0 or len2 == 0:
        return 0.0
    match_dist = max(len1, len2) // 2 - 1
    s1_matches = [False] * len1
    s2_matches = [False] * len2
    matches = 0
    transpositions = 0
    for i in range(len1):
        start = max(0, i - match_dist)
        end   = min(i + match_dist + 1, len2)
        for j in range(start, end):
            if s2_matches[j] or s1[i] != s2[j]:
                continue
            s1_matches[i] = s2_matches[j] = True
            matches += 1
            break
    if matches == 0:
        return 0.0
    k = 0
    for i in range(len1):
        if not s1_matches[i]:
            continue
        while not s2_matches[k]:
            k += 1
        if s1[i] != s2[k]:
            transpositions += 1
        k += 1
    return (matches / len1 + matches / len2 + (matches - transpositions / 2) / matches) / 3


def jaro_winkler(s1: str, s2: str, p: float = 0.1) -> float:
    jaro = _jaro(s1, s2)
    s1l, s2l = s1.lower().strip(), s2.lower().strip()
    prefix = 0
    for c1, c2 in zip(s1l[:4], s2l[:4]):
        if c1 == c2:
            prefix += 1
        else:
            break
    return jaro + prefix * p * (1 - jaro)


# ── Foursquare search ─────────────────────────────────────────────────────────

SESSION = requests.Session()

def fsq_search(name: str, lat: float, lon: float) -> dict | None:
    """Search Foursquare for a venue. Returns best match dict or None."""
    url = "https://places-api.foursquare.com/places/search"
    headers = {
        "Authorization": f"Bearer {FSQ_API_KEY}",
        "X-Places-Api-Version": "2025-06-17",
    }
    params = {
        "query":  name,
        "ll":     f"{lat},{lon}",
        "radius": SEARCH_RADIUS,
        "limit":  1,
    }
    try:
        resp = SESSION.get(url, headers=headers, params=params, timeout=10)
        if resp.status_code == 200:
            results = resp.json().get("results", [])
            return results[0] if results else None
        elif resp.status_code == 401:
            log.error("Invalid Foursquare API key — set FSQ_API_KEY correctly")
            sys.exit(1)
        elif resp.status_code == 429:
            log.warning("Rate limited — sleeping 5s")
            time.sleep(5)
            return None
        else:
            log.warning("FSQ %s for %r: %s", resp.status_code, name, resp.text[:100])
            return None
    except requests.RequestException as e:
        log.warning("Request error for %r: %s", name, e)
        return None


# ── Main ──────────────────────────────────────────────────────────────────────

PHOTOS_DIR = os.environ.get("PHOTOS_DIR", "/photos")
SIM_THRESHOLD = 0.80


def count_local_photos(place_id: str) -> int:
    """Count .jpg files already on disk for this restaurant."""
    d = os.path.join(PHOTOS_DIR, place_id)
    if not os.path.isdir(d):
        return 0
    return sum(1 for f in os.listdir(d) if f.endswith(".jpg"))


def main():
    if not FSQ_API_KEY:
        print("ERROR: Set FSQ_API_KEY environment variable")
        print("Get a free key at: https://developer.foursquare.com")
        sys.exit(1)

    conn = psycopg2.connect(DATABASE_URL)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT
                r.place_id,
                r.name,
                r.latitude,
                r.longitude,
                COALESCE(
                    jsonb_array_length(sd.raw_response->'place_results'->'images'),
                    0
                ) AS serpapi_image_count
            FROM   restaurants r
            LEFT JOIN serpapi_details sd ON sd.place_id = r.place_id
            WHERE  r.latitude IS NOT NULL
              AND  r.longitude IS NOT NULL
              AND  r.pipeline_status = 'complete'
            ORDER  BY RANDOM()
            LIMIT  %s
        """, (SAMPLE_SIZE,))
        restaurants = cur.fetchall()
    conn.close()

    log.info("Sampled %d restaurants — starting Foursquare lookup", len(restaurants))

    results = []

    for i, r in enumerate(restaurants, 1):
        name            = r["name"]
        lat             = float(r["latitude"])
        lon             = float(r["longitude"])
        serpapi_count   = r["serpapi_image_count"] or 0
        local_count     = count_local_photos(r["place_id"])

        match = fsq_search(name, lat, lon)
        time.sleep(REQUEST_DELAY)

        if match:
            fsq_name = match.get("name", "")
            sim      = jaro_winkler(name, fsq_name)
            fsq_ok   = sim >= SIM_THRESHOLD
        else:
            fsq_name = None
            sim      = None
            fsq_ok   = False

        results.append({
            "name":           name,
            "place_id":       r["place_id"],
            "serpapi_count":  serpapi_count,
            "local_count":    local_count,
            "fsq_name":       fsq_name,
            "sim":            sim,
            "fsq_ok":         fsq_ok,
        })

        fsq_status = f"✓ {fsq_name!r} ({sim:.2f})" if fsq_name else "✗ not found"
        if fsq_name and not fsq_ok:
            fsq_status += " [below threshold]"
        log.info("[%3d/%d] %-40s serp=%d local=%d fsq=%s",
                 i, len(restaurants), name[:40], serpapi_count, local_count, fsq_status)

    # ── Report ────────────────────────────────────────────────────────────────
    sims = [r["sim"] for r in results if r["sim"] is not None]
    fsq_confident  = [r for r in results if r["fsq_ok"]]
    fsq_found_all  = [r for r in results if r["fsq_name"] is not None]
    has_serpapi    = [r for r in results if r["serpapi_count"] > 0]
    has_local      = [r for r in results if r["local_count"] > 0]
    covered        = [r for r in results if r["fsq_ok"] or r["serpapi_count"] > 0]
    covered_either = [r for r in results if r["fsq_ok"] or r["local_count"] > 0]

    print("\n" + "="*70)
    print("PHOTO COVERAGE: FOURSQUARE vs. SERPAPI  (threshold ≥ 0.80)")
    print("="*70)
    print(f"Sample size              : {len(results)}")
    print()
    print(f"── Foursquare ───────────────────────────────────────────────────────")
    print(f"  Found (any sim)        : {len(fsq_found_all):3d} / {len(results)}  ({len(fsq_found_all)/len(results)*100:.0f}%)")
    print(f"  Confident (≥ 0.80)     : {len(fsq_confident):3d} / {len(results)}  ({len(fsq_confident)/len(results)*100:.0f}%)")
    if sims:
        print(f"  JW mean / median       : {statistics.mean(sims):.3f} / {statistics.median(sims):.3f}")
    print()
    print(f"── SerpAPI (stored) ─────────────────────────────────────────────────")
    print(f"  Has images in DB       : {len(has_serpapi):3d} / {len(results)}  ({len(has_serpapi)/len(results)*100:.0f}%)")
    print(f"  Has local photo files  : {len(has_local):3d} / {len(results)}  ({len(has_local)/len(results)*100:.0f}%)")
    if has_serpapi:
        avg = statistics.mean(r["serpapi_count"] for r in has_serpapi)
        print(f"  Avg images (wenn vorhanden): {avg:.1f}")
    print()
    print(f"── Kombiniert ───────────────────────────────────────────────────────")
    print(f"  FSQ ≥0.80 ODER SerpAPI : {len(covered):3d} / {len(results)}  ({len(covered)/len(results)*100:.0f}%)")
    print(f"  FSQ ≥0.80 ODER lokal   : {len(covered_either):3d} / {len(results)}  ({len(covered_either)/len(results)*100:.0f}%)")
    no_coverage = [r for r in results if not r["fsq_ok"] and r["serpapi_count"] == 0 and r["local_count"] == 0]
    print(f"  Keine Quelle           : {len(no_coverage):3d} / {len(results)}  ({len(no_coverage)/len(results)*100:.0f}%)")

    # Per-restaurant table for ≥ 0.80 FSQ matches
    print(f"\n── Restaurants mit FSQ ≥ 0.80  ({len(fsq_confident)} Stück) ──────────────────────")
    print(f"  {'Name':<38} {'FSQ Name':<28} {'sim':>5}  {'serp':>4}  {'lok':>3}")
    print(f"  {'-'*38} {'-'*28} {'-'*5}  {'-'*4}  {'-'*3}")
    for r in sorted(fsq_confident, key=lambda x: -x["sim"]):
        print(f"  {r['name'][:38]:<38} {(r['fsq_name'] or '')[:28]:<28} {r['sim']:.3f}  {r['serpapi_count']:4d}  {r['local_count']:3d}")

    print(f"\n── Restaurants OHNE FSQ-Match und OHNE SerpAPI-Bilder ({len(no_coverage)} Stück) ──")
    for r in no_coverage:
        print(f"  {r['name']}")

    print("="*70)


if __name__ == "__main__":
    main()
