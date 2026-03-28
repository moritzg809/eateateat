"""
cuisine_city_dna.py — kombinierter wPMI + PCA/DBSCAN Ansatz

Pipeline pro Stadt:
  1. wPMI (SQL): findet die distinktiven cuisine_types einer Stadt
  2. Embedding (Jina): embedded die Top-N distinctiven Typen
  3. PCA deflation: wirft das Grundrauschen raus
  4. DBSCAN: clustert semantisch ähnliche Varianten zusammen
  5. Cluster-Label = häufigster Typ im Cluster (+ wPMI-Score)

Ergebnis: pro Stadt eine sortierte Liste von Filter-Vorschlägen,
          dedupliziert und bedeutungstragend.

Usage:
    python cuisine_city_dna.py
    python cuisine_city_dna.py --top-n 30
    python cuisine_city_dna.py --min-count 3 --max-cities 2
    python cuisine_city_dna.py --eps 0.25 --pca-dims 3
    python cuisine_city_dna.py --write   # write results to city_cuisine_labels table
"""

import argparse
import logging
import os
import sys
from collections import defaultdict

import numpy as np
import psycopg2
import psycopg2.extras

logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Defaults
TOP_N      = 40    # Top-N distinctive cuisine_types per city als Input
MIN_COUNT  = 3     # Minimum Restaurants damit ein Typ berücksichtigt wird
MAX_CITIES = 3     # Nur Typen die in max. N Städten vorkommen (Exklusivität)
MIN_WPMI   = 1.0   # Singletons unter diesem Wert werden nicht angezeigt
PCA_DIMS   = 5     # Zu entfernende dominante Dimensionen
EPS_DEDUP  = 0.12  # Sehr eng: nur echte String-Duplikate / Schreibvarianten
EPS_GROUP  = 0.20  # Weiter: konzeptuelle Verwandtschaft
MIN_SAMP   = 2     # DBSCAN min_samples


# ── DB ────────────────────────────────────────────────────────────────────────

def get_connection():
    url = os.environ.get("DATABASE_URL")
    if not url:
        sys.exit("ERROR: DATABASE_URL not set")
    return psycopg2.connect(url)


def fetch_city_ids(conn) -> dict[str, int]:
    """Returns {city_name: city_id} for all cities."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT id, name FROM cities")
        return {r["name"]: r["id"] for r in cur.fetchall()}


def save_labels(conn, city_id: int, suggestions: list[dict]) -> None:
    """Writes the filtered suggestions for one city to city_cuisine_labels."""
    with conn.cursor() as cur:
        # Clear old labels for this city first
        cur.execute("DELETE FROM city_cuisine_labels WHERE city_id = %s", (city_id,))
        psycopg2.extras.execute_batch(
            cur,
            """
            INSERT INTO city_cuisine_labels
                (city_id, label, cuisine_types, wpmi, restaurant_n, pct_of_global, computed_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (city_id, label) DO UPDATE SET
                cuisine_types  = EXCLUDED.cuisine_types,
                wpmi           = EXCLUDED.wpmi,
                restaurant_n   = EXCLUDED.restaurant_n,
                pct_of_global  = EXCLUDED.pct_of_global,
                computed_at    = NOW()
            """,
            [
                (city_id, s["label"], s["all_variants"],
                 s["wpmi"], s["total_n"], s["pct_of_global"])
                for s in suggestions
            ],
        )
    conn.commit()


def fetch_wpmi(conn, min_count: int, max_cities: int) -> list[dict]:
    """Berechnet wPMI pro (city, cuisine_type) und gibt alle Typen zurück."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            WITH city_counts AS (
                SELECT ci.name AS city, e.cuisine_type, COUNT(*) AS n
                FROM gemini_enrichments e
                JOIN top_restaurants t ON t.place_id = e.place_id
                JOIN cities ci         ON ci.id       = t.city_id
                WHERE e.cuisine_type IS NOT NULL AND e.cuisine_type != ''
                GROUP BY ci.name, e.cuisine_type
            ),
            city_totals   AS (SELECT city, SUM(n) AS total FROM city_counts GROUP BY city),
            global_totals AS (SELECT SUM(n) AS total FROM city_counts),
            global_counts AS (
                SELECT cuisine_type, SUM(n) AS n, COUNT(DISTINCT city) AS n_cities
                FROM city_counts GROUP BY cuisine_type
            )
            SELECT
                cc.city,
                cc.cuisine_type,
                cc.n                                                      AS city_n,
                gc.n                                                      AS global_n,
                gc.n_cities,
                ROUND(100.0 * cc.n / gc.n, 0)                            AS pct_of_global,
                ROUND((
                    LN((cc.n::float / ct.total) / (gc.n::float / gt.total))
                    * LN(cc.n + 1)
                )::numeric, 3)                                            AS wpmi
            FROM city_counts cc
            JOIN city_totals ct   ON ct.city         = cc.city
            JOIN global_counts gc ON gc.cuisine_type = cc.cuisine_type
            JOIN global_totals gt ON true
            WHERE cc.n >= %(min_count)s
              AND gc.n_cities <= %(max_cities)s
            ORDER BY cc.city, wpmi DESC
        """, {"min_count": min_count, "max_cities": max_cities})
        return [dict(r) for r in cur.fetchall()]


# ── Embedding + PCA + DBSCAN ──────────────────────────────────────────────────

def load_model():
    from sentence_transformers import SentenceTransformer
    return SentenceTransformer("jinaai/jina-embeddings-v5-text-small-retrieval", trust_remote_code=True)


def embed(model, texts: list[str]) -> np.ndarray:
    vecs = model.encode(texts, task="text-matching", show_progress_bar=False, batch_size=64)
    vecs = np.array(vecs, dtype=np.float32)
    norms = np.linalg.norm(vecs, axis=1, keepdims=True)
    return vecs / np.where(norms == 0, 1.0, norms)


def deflate_pca(vecs: np.ndarray, n: int) -> np.ndarray:
    """Entfernt Top-N PCs und renormalisiert."""
    if n <= 0 or len(vecs) <= n:
        return vecs
    from sklearn.decomposition import PCA
    pca = PCA(n_components=n)
    pca.fit(vecs)
    residual = vecs - (vecs @ pca.components_.T) @ pca.components_
    norms = np.linalg.norm(residual, axis=1, keepdims=True)
    return residual / np.where(norms == 0, 1.0, norms)


def cluster_dbscan(vecs: np.ndarray, eps: float, min_samples: int) -> np.ndarray:
    """DBSCAN auf Cosine-Distanz. Gut für Dedup (findet dichte Punkte, Noise=-1)."""
    from sklearn.cluster import DBSCAN
    sim  = vecs @ vecs.T
    dist = np.clip(1.0 - sim, 0.0, 2.0).astype(np.float64)
    return DBSCAN(eps=eps, min_samples=min_samples, metric="precomputed").fit_predict(dist)


def cluster_agglomerative(vecs: np.ndarray, distance_threshold: float) -> np.ndarray:
    """Average-Linkage Clustering auf Cosine-Distanz.

    Verhindert DBSCAN-Chaining: A≈B und B≈C führt NICHT zu A+B+C wenn A-C weit.
    Average-Linkage fordert dass der *Durchschnitts*abstand im Cluster ≤ threshold.
    Gibt Labels zurück; Singleton-Cluster haben jeweils eigenen Label.
    """
    from sklearn.cluster import AgglomerativeClustering
    if len(vecs) < 2:
        return np.zeros(len(vecs), dtype=int)
    sim  = vecs @ vecs.T
    dist = np.clip(1.0 - sim, 0.0, 2.0).astype(np.float64)
    model = AgglomerativeClustering(
        n_clusters=None,
        metric="precomputed",
        linkage="average",
        distance_threshold=distance_threshold,
    )
    return model.fit_predict(dist)


# ── Cluster-Label-Wahl ────────────────────────────────────────────────────────

def pick_label(members: list[dict]) -> str:
    """Nimmt den Typ mit dem höchsten city_n als Cluster-Label."""
    return max(members, key=lambda x: x["city_n"])["cuisine_type"]


# ── Report ────────────────────────────────────────────────────────────────────

W = 68

def header(t):  print(f"\n{'═'*W}\n  {t}\n{'═'*W}")
def section(t): print(f"\n{'─'*W}\n  {t}\n{'─'*W}")


def print_city(city: str, suggestions: list[dict]):
    total_restaurants = sum(s["total_n"] for s in suggestions)
    section(f"{city}  —  {len(suggestions)} Konzepte  ({total_restaurants} Restaurants)")

    for s in suggestions:
        excl    = "⭐" if s["pct_of_global"] >= 90 else "  "
        grouped = "⊕" if s["grouped"] else " "
        # Zeige Varianten (dedup + konzept-merge)
        vars_without_label = [v for v in s["all_variants"] if v != s["label"]]
        vars_str = ""
        if vars_without_label:
            vars_str = f"  ← {', '.join(vars_without_label[:4])}" + (" …" if len(vars_without_label) > 4 else "")
        print(f"  {excl}{grouped} {s['wpmi']:>5.2f}  {s['total_n']:>3}×  {s['label']:<36}{vars_str}")


# ── Main ──────────────────────────────────────────────────────────────────────

def run(top_n: int, min_count: int, max_cities: int, pca_dims: int,
        eps: float = EPS_GROUP, write: bool = False):
    conn = get_connection()
    rows = fetch_wpmi(conn, min_count, max_cities)
    city_ids = fetch_city_ids(conn) if write else {}
    if not write:
        conn.close()
        conn = None

    if not rows:
        sys.exit("Keine Daten gefunden.")

    # Gruppiere nach Stadt, nimm Top-N
    by_city: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_city[r["city"]].append(r)

    # Alle einzigartigen cuisine_types einmal embedden (effizienter)
    all_types = list({r["cuisine_type"] for r in rows})
    print(f"Lade Jina Modell … (embedding {len(all_types)} cuisine_types)")
    model     = load_model()
    all_vecs  = embed(model, all_types)
    type2vec  = dict(zip(all_types, all_vecs))

    header("CUISINE CITY DNA  —  wPMI + PCA/DBSCAN")
    print(f"  Städte          : {len(by_city)}")
    print(f"  Top-N per Stadt : {top_n}")
    print(f"  Min. count      : {min_count}")
    print(f"  Max. cities     : {max_cities}  (Exklusivitäts-Filter)")
    print(f"  PCA dims raus   : {pca_dims}")
    print(f"  DBSCAN dedup    : eps={EPS_DEDUP}  (sim ≥ {1-EPS_DEDUP:.2f}  — Schreibvarianten)")
    print(f"  DBSCAN grouping : eps={eps}  (sim ≥ {1-eps:.2f}  — konzeptuell verwandt)")

    for city in sorted(by_city.keys()):
        city_rows = by_city[city][:top_n]
        types     = [r["cuisine_type"] for r in city_rows]
        wpmi_map  = {r["cuisine_type"]: float(r["wpmi"])         for r in city_rows}
        count_map = {r["cuisine_type"]: int(r["city_n"])         for r in city_rows}
        global_map= {r["cuisine_type"]: int(r["pct_of_global"])  for r in city_rows}

        if len(types) < 2:
            continue

        # Embeddings für diese Stadt-Typen
        vecs     = np.stack([type2vec[t] for t in types])

        # PCA deflation — entfernt "alle sind Essen"-Signal
        deflated = deflate_pca(vecs, min(pca_dims, len(types) - 1))

        # Schritt 1 — Deduplication: DBSCAN, sehr eng (Schreibvarianten)
        dedup_labels = cluster_dbscan(deflated, EPS_DEDUP, MIN_SAMP)

        # Erst dedupen: Cluster → ein repräsentativer Typ (höchster count)
        dedup_groups: dict[int, list[str]] = defaultdict(list)
        for t, lbl in zip(types, dedup_labels):
            dedup_groups[lbl].append(t)

        # Erzeuge deduplizierte Typen-Liste (einer pro Gruppe, Noise bleibt einzeln)
        deduped: list[dict] = []
        for lbl, group in dedup_groups.items():
            if lbl == -1:
                # Noise: jeder Typ für sich
                for t in group:
                    deduped.append({
                        "cuisine_type": t,
                        "variants":     [t],
                        "city_n":       count_map[t],
                        "wpmi":         wpmi_map[t],
                        "pct_of_global": global_map[t],
                    })
            else:
                # Gruppe: repräsentant = höchster count, n = Summe
                rep = max(group, key=lambda t: count_map[t])
                deduped.append({
                    "cuisine_type": rep,
                    "variants":     sorted(group, key=lambda t: -count_map[t]),
                    "city_n":       sum(count_map[t] for t in group),
                    "wpmi":         max(wpmi_map[t] for t in group),
                    "pct_of_global": max(global_map[t] for t in group),
                })

        # Schritt 2 — Konzept-Grouping: Original-Embeddings (kein PCA)
        # Hier wollen wir kulturelle Nähe erhalten, nicht wegdeflationieren.
        # "Schwäbisch" ≈ "Schwäbische Küche" ≈ "Schwäbische Besenwirtschaft"
        # aber "Schwäbisch" ≠ "Thailändisch"
        if len(deduped) >= 2:
            dedup_types  = [d["cuisine_type"] for d in deduped]
            dedup_vecs   = np.stack([type2vec[t] for t in dedup_types])
            # Average-Linkage: kein Chaining, Durchschnittsabstand muss ≤ eps
            group_labels = cluster_agglomerative(dedup_vecs, EPS_GROUP)
        else:
            group_labels = np.zeros(len(deduped), dtype=int)

        # Baue finale Suggestions
        concept_groups: dict[int, list[dict]] = defaultdict(list)
        for d, lbl in zip(deduped, group_labels):
            concept_groups[lbl].append(d)

        suggestions = []
        for lbl, members in concept_groups.items():
            members_sorted = sorted(members, key=lambda x: -x["city_n"])
            label      = max(members_sorted, key=lambda x: x["city_n"])["cuisine_type"]
            all_vars   = [v for m in members_sorted for v in m["variants"]]
            total_n    = sum(m["city_n"] for m in members_sorted)
            best_wpmi  = max(m["wpmi"] for m in members_sorted)
            best_pct   = max(m["pct_of_global"] for m in members_sorted)
            is_grouped = len(members) > 1
            suggestions.append({
                "label":        label,
                "members":      members_sorted,
                "all_variants": all_vars,
                "total_n":      total_n,
                "wpmi":         best_wpmi,
                "pct_of_global": best_pct,
                "grouped":      is_grouped,
            })

        suggestions.sort(key=lambda x: -x["wpmi"])

        # Filtere: Cluster (> 1 member) immer zeigen; Singletons nur wenn wPMI hoch genug
        filtered = [s for s in suggestions if s["grouped"] or s["wpmi"] >= MIN_WPMI]
        print_city(city, filtered)

        if write:
            city_id = city_ids.get(city)
            if city_id is None:
                print(f"  ⚠  Stadt '{city}' nicht in cities-Tabelle — übersprungen")
            else:
                save_labels(conn, city_id, filtered)
                print(f"  ✓  {len(filtered)} Labels für '{city}' (id={city_id}) gespeichert")

    if write and conn:
        conn.close()
        print(f"\nFertig — city_cuisine_labels befüllt.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Cuisine City DNA — wPMI + PCA/DBSCAN")
    ap.add_argument("--top-n",      type=int,   default=TOP_N)
    ap.add_argument("--min-count",  type=int,   default=MIN_COUNT)
    ap.add_argument("--max-cities", type=int,   default=MAX_CITIES)
    ap.add_argument("--pca-dims",   type=int,   default=PCA_DIMS)
    ap.add_argument("--eps",        type=float, default=EPS_GROUP)
    ap.add_argument("--write",      action="store_true",
                    help="write results to city_cuisine_labels table (requires migration 013)")
    args = ap.parse_args()
    run(args.top_n, args.min_count, args.max_cities, args.pca_dims, args.eps, args.write)
