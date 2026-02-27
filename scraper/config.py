# =============================================================
# Scraper configuration
# Adjust SEARCH_TERMS and LOCATIONS freely — every combination
# of (term x location) maps to exactly one cached API call.
# =============================================================

SEARCH_TERMS = [
    "Specialty Coffee",
    "Brunch",
    "Wine Bar",
    "Bodega",
    "Vermuteria",
    "Restaurant",
    "Chiringuito",
    "Tapas modern",
    "Farm to table",
    "Rooftop bar",
    "Boutique hotel restaurant",
    "Sommelier",
    "Open kitchen",
    "Romantic terrace",
    "Dachterrasse",
    "Great view Restaurant",
    "Pa Amb Oli",
]

LOCATIONS = [
    "Mallorca",
    "Palma de Mallorca",
    "Portixol, Palma de Mallorca",
    "Santa Catalina, Palma de Mallorca",
    "Alaró, Mallorca",
    "Sóller, Mallorca",
    "Pollença, Mallorca",
]

# --- Step 2 thresholds (used by the top_restaurants DB view too) ---
MIN_RATING       = 4.5
MIN_RATING_COUNT = 100

# --- Serper API settings ---
SERPER_MAPS_URL   = "https://google.serper.dev/maps"
SERPER_PLACES_URL = "https://google.serper.dev/places"

SEARCH_LANGUAGE = "de"   # hl parameter
SEARCH_COUNTRY  = "es"   # gl parameter
RESULTS_PER_CALL = 20    # max supported by Serper Maps
