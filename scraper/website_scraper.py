"""
mallorcaeat — Restaurant Website Scraper

Crawls each restaurant's own website (all subpages) and:
  - Downloads images  → WEBSITE_SCRAPES_DIR/{place_id}/{place_id}-WebsiteScraper-{n}.jpg
  - Saves text        → WEBSITE_SCRAPES_DIR/{place_id}/content.txt
  - Saves metadata    → WEBSITE_SCRAPES_DIR/{place_id}/meta.json

Skips social media URLs (Instagram, Facebook, etc.) and previously scraped sites.

Usage:
    python website_scraper.py               # scrape all restaurants with websites
    python website_scraper.py --dry-run     # preview only
    python website_scraper.py --limit 10    # process at most 10 restaurants
    python website_scraper.py --force       # re-scrape even if already done
"""

import argparse
import hashlib
import io
import json
import logging
import os
import time
import re
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import urljoin, urlparse, urldefrag

import requests
from bs4 import BeautifulSoup
from PIL import Image

from db import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────

WEBSITE_SCRAPES_DIR = os.getenv("WEBSITE_SCRAPES_DIR", "/website_scrapes")
MAX_PAGES           = int(os.getenv("WS_MAX_PAGES", "100"))
MAX_IMAGES          = int(os.getenv("WS_MAX_IMAGES", "20"))
MAX_CRAWL_DEPTH     = int(os.getenv("WS_MAX_DEPTH", "5"))
REQUEST_TIMEOUT     = 15
PAGE_DELAY          = 0.3   # seconds between page request batches
MIN_IMAGE_BYTES     = 10_000  # skip images smaller than 10 KB
WS_WORKERS          = int(os.getenv("WS_WORKERS", "10"))   # parallel restaurants (different domains)
WS_PAGE_WORKERS     = int(os.getenv("WS_PAGE_WORKERS", "4"))  # parallel pages per restaurant (same domain — keep low)
WS_IMG_WORKERS      = int(os.getenv("WS_IMG_WORKERS", "10"))  # parallel image downloads (mixed domains)

PAGE_CACHE_DIR      = os.path.join(WEBSITE_SCRAPES_DIR, "_page_cache")

_SOCIAL_DOMAINS = {
    "instagram.com", "facebook.com", "tripadvisor.com", "tripadvisor.es",
    "twitter.com", "x.com", "tiktok.com", "youtube.com", "yelp.com",
    "google.com", "maps.google.com", "booking.com", "airbnb.com",
}

_SKIP_IMAGE_PATTERNS = re.compile(
    r"(logo|icon|favicon|avatar|placeholder|spinner|loading|pixel|blank|"
    r"arrow|button|bg[-_]|background|pattern|flag|star|rating|badge|"
    r"social|facebook|twitter|instagram|whatsapp|\.gif$)",
    re.IGNORECASE,
)

_SKIP_URL_EXTENSIONS = {".pdf", ".doc", ".docx", ".zip", ".mp4", ".mp3", ".xml", ".rss"}

# Path segments that indicate unrelated content — skip these to avoid crawling blogs etc.
_SKIP_PATH_SEGMENTS = re.compile(
    r"^/(blog|news|press|presse|noticias|novedades|eventos|events|team|equipo|"
    r"careers|jobs|empleo|stellenangebote|datenschutz|impressum|aviso-legal|"
    r"privacy|cookie|politique|agb|terms|legal|sitemap)(/|$)",
    re.IGNORECASE,
)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "de,es;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

_thread_local = threading.local()


def _get_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        s.headers.update(_HEADERS)
        _thread_local.session = s
    return _thread_local.session


# ── URL helpers ───────────────────────────────────────────────────────────────

def _is_social(url: str) -> bool:
    try:
        domain = urlparse(url).netloc.lstrip("www.")
        return any(domain == s or domain.endswith("." + s) for s in _SOCIAL_DOMAINS)
    except Exception:
        return False


def _same_domain(url: str, base_netloc: str) -> bool:
    try:
        return urlparse(url).netloc == base_netloc
    except Exception:
        return False


def _is_unwanted_path(url: str) -> bool:
    """Skip blog, news, legal pages and other unrelated sections."""
    path = urlparse(url).path
    return bool(_SKIP_PATH_SEGMENTS.match(path))


def _clean_url(url: str) -> str:
    url, _ = urldefrag(url)
    return url.rstrip("/")


def _skip_extension(url: str) -> bool:
    path = urlparse(url).path.lower()
    return any(path.endswith(ext) for ext in _SKIP_URL_EXTENSIONS)


# ── Persistent page cache ─────────────────────────────────────────────────────

def _cache_path(url: str) -> str:
    key = hashlib.sha256(url.encode()).hexdigest()
    return os.path.join(PAGE_CACHE_DIR, f"{key}.html")


def _cache_get(url: str) -> str | None:
    path = _cache_path(url)
    if os.path.exists(path):
        with open(path, encoding="utf-8", errors="replace") as f:
            return f.read()
    return None


def _cache_put(url: str, html: str) -> None:
    os.makedirs(PAGE_CACHE_DIR, exist_ok=True)
    with open(_cache_path(url), "w", encoding="utf-8", errors="replace") as f:
        f.write(html)


# ── Text extraction ───────────────────────────────────────────────────────────

def _extract_text(soup: BeautifulSoup, url: str) -> str:
    """Extract meaningful text from a page."""
    # Remove noise tags
    for tag in soup(["script", "style", "noscript", "nav", "footer",
                     "header", "aside", "form", "svg", "iframe"]):
        tag.decompose()

    lines = []

    # Title
    title = soup.find("title")
    if title and title.get_text(strip=True):
        lines.append(f"# {title.get_text(strip=True)}")

    # Meta description
    meta = soup.find("meta", attrs={"name": "description"})
    if not meta:
        meta = soup.find("meta", property="og:description")
    if meta and meta.get("content", "").strip():
        lines.append(meta["content"].strip())

    # Main content — prefer semantic containers
    main = (
        soup.find("main")
        or soup.find("article")
        or soup.find(id=re.compile(r"(content|main|body)", re.I))
        or soup.find(class_=re.compile(r"(content|main|body)", re.I))
        or soup.body
    )
    if main:
        for tag in main.find_all(["h1", "h2", "h3", "p", "li"]):
            text = tag.get_text(" ", strip=True)
            if len(text) > 20:
                lines.append(text)

    return "\n\n".join(dict.fromkeys(lines))  # deduplicate while preserving order


# ── Image extraction ──────────────────────────────────────────────────────────

def _extract_image_urls(soup: BeautifulSoup, page_url: str) -> list[str]:
    """Return candidate image URLs from a page, best-first."""
    seen = set()
    candidates = []

    def add(url):
        url = urljoin(page_url, url)
        url, _ = urldefrag(url)
        if url not in seen and url.startswith("http"):
            seen.add(url)
            candidates.append(url)

    # 1. OG image (highest quality)
    og = soup.find("meta", property="og:image")
    if og and og.get("content"):
        add(og["content"])

    # 2. Schema.org image
    for tag in soup.find_all("meta", attrs={"itemprop": "image"}):
        if tag.get("content"):
            add(tag["content"])

    # 3. Regular img tags — prefer those in main content areas
    containers = (
        soup.find_all(["main", "article", "section"])
        or [soup.body or soup]
    )
    for container in containers:
        for img in container.find_all("img"):
            src = img.get("src") or img.get("data-src") or img.get("data-lazy-src")
            if not src:
                continue
            if _SKIP_IMAGE_PATTERNS.search(src):
                continue
            # Heuristic: skip tiny images via width/height attributes
            try:
                w = int(img.get("width", 0))
                h = int(img.get("height", 0))
                if (w and w < 100) or (h and h < 100):
                    continue
            except (ValueError, TypeError):
                pass
            add(src)

    # 4. Srcset (responsive images)
    for img in soup.find_all("img", srcset=True):
        for part in img["srcset"].split(","):
            part = part.strip().split()[0]
            if part:
                add(part)

    return candidates


# ── Image download ────────────────────────────────────────────────────────────

def _download_image(url: str, dest_path: str) -> bool:
    """Download and convert image to JPEG. Returns True on success."""
    try:
        resp = _get_session().get(url, timeout=REQUEST_TIMEOUT, stream=True)
        resp.raise_for_status()
        raw = resp.content
        if len(raw) < MIN_IMAGE_BYTES:
            return False
        img = Image.open(io.BytesIO(raw)).convert("RGB")
        # Skip if image dimensions are too small
        if img.width < 200 or img.height < 150:
            return False
        img.save(dest_path, "JPEG", quality=85, optimize=True)
        return True
    except Exception as exc:
        logger.debug("Image download failed (%s): %s", url, exc)
        return False


# ── Page crawler ──────────────────────────────────────────────────────────────

def _fetch_page(url: str) -> tuple[str, str | None, bool]:
    """Fetch a single page. Returns (url, html, cache_hit)."""
    html = _cache_get(url)
    if html is not None:
        return url, html, True
    try:
        resp = _get_session().get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        if resp.status_code != 200:
            return url, None, False
        if "text/html" not in resp.headers.get("content-type", ""):
            return url, None, False
        html = resp.text
        _cache_put(url, html)
        return url, html, False
    except Exception as exc:
        logger.debug("Failed to fetch %s: %s", url, exc)
        return url, None, False


def crawl_website(start_url: str) -> dict:
    """
    BFS crawl of all internal pages using parallel batch fetching. Returns:
      {
        "pages_visited": [...],
        "text": "...",
        "image_urls": [...],
      }

    Scope: same domain as start_url, skipping blog/news/legal path segments.
    Page HTML is cached to disk so repeated runs never re-fetch.
    """
    base_netloc = urlparse(start_url).netloc
    queue = deque([(start_url, 0)])  # (url, depth)
    visited = set()
    all_text_parts = []
    all_image_urls = []
    seen_image_urls = set()
    pages_visited = []

    with ThreadPoolExecutor(max_workers=WS_PAGE_WORKERS) as executor:
        while queue and len(pages_visited) < MAX_PAGES:
            # Collect a batch of URLs to fetch in parallel
            batch: list[tuple[str, int]] = []
            while queue and len(batch) < WS_PAGE_WORKERS and len(pages_visited) + len(batch) < MAX_PAGES:
                url, depth = queue.popleft()
                url = _clean_url(url)
                if url in visited or _skip_extension(url):
                    continue
                visited.add(url)
                batch.append((url, depth))

            if not batch:
                continue

            futures = {executor.submit(_fetch_page, url): (url, depth) for url, depth in batch}
            has_uncached = False

            for future in as_completed(futures):
                url, depth = futures[future]
                _, html, cache_hit = future.result()
                if html is None:
                    continue
                if not cache_hit:
                    has_uncached = True

                soup = BeautifulSoup(html, "lxml")
                pages_visited.append(url)
                logger.debug("  %s %s", "CACHE" if cache_hit else "FETCH", url)

                # Collect text
                text = _extract_text(soup, url)
                if text:
                    all_text_parts.append(f"[PAGE: {url}]\n{text}")

                # Collect images
                for img_url in _extract_image_urls(soup, url):
                    if img_url not in seen_image_urls:
                        seen_image_urls.add(img_url)
                        all_image_urls.append(img_url)

                # Follow internal links if not at max depth
                if depth < MAX_CRAWL_DEPTH:
                    for a in soup.find_all("a", href=True):
                        href = urljoin(url, a["href"])
                        href = _clean_url(href)
                        if (href not in visited
                                and _same_domain(href, base_netloc)
                                and not _skip_extension(href)
                                and not _is_social(href)
                                and not _is_unwanted_path(href)):
                            queue.append((href, depth + 1))

            if has_uncached:
                time.sleep(PAGE_DELAY)

    return {
        "pages_visited": pages_visited,
        "text": "\n\n---\n\n".join(all_text_parts),
        "image_urls": all_image_urls,
    }


# ── Scrape one restaurant ─────────────────────────────────────────────────────

def scrape_restaurant(place_id: str, name: str, website: str, dry_run: bool = False) -> dict:
    dest_dir = os.path.join(WEBSITE_SCRAPES_DIR, place_id)
    meta_path = os.path.join(dest_dir, "meta.json")

    if dry_run:
        logger.info("  DRY-RUN: would scrape %s", website)
        return {"status": "dry_run"}

    os.makedirs(dest_dir, exist_ok=True)

    logger.info("  Crawling %s", website)
    result = crawl_website(website)

    pages = result["pages_visited"]
    logger.info("  Visited %d page(s)", len(pages))

    # Save text
    if result["text"]:
        with open(os.path.join(dest_dir, "content.txt"), "w", encoding="utf-8") as f:
            f.write(result["text"])

    # Download images in parallel
    candidates = result["image_urls"][:MAX_IMAGES * 3]  # fetch more candidates to reach MAX_IMAGES

    def _try_download(args: tuple[int, str]) -> tuple[int, bool]:
        candidate_idx, img_url = args
        tmp_path = os.path.join(dest_dir, f"_tmp_{candidate_idx}.jpg")
        ok = _download_image(img_url, tmp_path)
        return candidate_idx, ok

    with ThreadPoolExecutor(max_workers=WS_IMG_WORKERS) as executor:
        futures = {executor.submit(_try_download, (i, url)): i for i, url in enumerate(candidates)}
        results_map: dict[int, bool] = {}
        for future in as_completed(futures):
            idx, ok = future.result()
            results_map[idx] = ok

    # Rename successful downloads to final names in order
    downloaded = 0
    for i in range(len(candidates)):
        if downloaded >= MAX_IMAGES:
            # Remove unused temp files
            tmp = os.path.join(dest_dir, f"_tmp_{i}.jpg")
            if os.path.exists(tmp):
                os.remove(tmp)
            continue
        tmp = os.path.join(dest_dir, f"_tmp_{i}.jpg")
        if results_map.get(i) and os.path.exists(tmp):
            final = os.path.join(dest_dir, f"{place_id}-WebsiteScraper-{downloaded}.jpg")
            os.rename(tmp, final)
            downloaded += 1
        elif os.path.exists(tmp):
            os.remove(tmp)

    logger.info("  Downloaded %d image(s)", downloaded)

    # Save metadata
    meta = {
        "place_id":      place_id,
        "name":          name,
        "website":       website,
        "scraped_at":    datetime.utcnow().isoformat(),
        "pages_visited": pages,
        "images_saved":  downloaded,
    }
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    return {"status": "ok", "pages": len(pages), "images": downloaded}


# ── Main / run ────────────────────────────────────────────────────────────────

def _already_scraped(place_id: str) -> bool:
    meta_path = os.path.join(WEBSITE_SCRAPES_DIR, place_id, "meta.json")
    return os.path.exists(meta_path)


def run(limit: int | None = None, dry_run: bool = False, force: bool = False):
    """Scrape websites for all complete restaurants. Idempotent."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT place_id, name, website
                FROM   restaurants
                WHERE  pipeline_status = 'complete'
                  AND  website IS NOT NULL
                ORDER  BY name
            """)
            rows = cur.fetchall()

    if limit:
        rows = rows[:limit]

    total     = len(rows)
    skipped   = 0
    processed = 0
    errors    = 0

    logger.info("Found %d restaurants with scrapeable websites (workers=%d)", total, WS_WORKERS)

    todo = []
    for i, (place_id, name, website) in enumerate(rows, 1):
        if not force and _already_scraped(place_id):
            skipped += 1
            continue
        todo.append((i, place_id, name, website))

    counter_lock = threading.Lock()

    def _process(args: tuple[int, str, str, str]) -> str:
        nonlocal processed, errors
        i, place_id, name, website = args
        prefix = f"[{i:>4}/{total}]"
        logger.info("%s %s", prefix, name)
        try:
            result = scrape_restaurant(place_id, name, website, dry_run=dry_run)
            if result.get("status") == "ok":
                logger.info("%s → %d pages, %d images", prefix, result["pages"], result["images"])
            with counter_lock:
                processed += 1
            return "ok"
        except Exception as exc:
            logger.warning("%s → ERROR: %s", prefix, exc)
            with counter_lock:
                errors += 1
            return "error"

    with ThreadPoolExecutor(max_workers=WS_WORKERS) as executor:
        list(executor.map(_process, todo))

    logger.info(
        "Done — %d processed, %d skipped (already done), %d errors",
        processed, skipped, errors,
    )


def main():
    ap = argparse.ArgumentParser(description="Scrape restaurant websites for text and images")
    ap.add_argument("--dry-run", action="store_true", help="Preview without downloading")
    ap.add_argument("--limit",   type=int, default=None, help="Max restaurants to process")
    ap.add_argument("--force",   action="store_true",    help="Re-scrape even if already done")
    args = ap.parse_args()
    run(limit=args.limit, dry_run=args.dry_run, force=args.force)


if __name__ == "__main__":
    main()
