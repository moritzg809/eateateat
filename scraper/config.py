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
    "Kaffee und Kuchen",
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
    "Mandelkuchen",
    "familiengeführtes restaurant",
    "Slow food",
    "Cocina mallorquina",
    "Ensaimada artesanal",
    "Natural wine bar",
    "Aperitivo",
    "Garden restaurant",
    "Sushi restaurant",
    "Fusion restaurant",
]

LOCATIONS = [
    "Mallorca",
    "Palma de Mallorca",
    "Portixol, Palma de Mallorca",
    "Santa Catalina, Palma de Mallorca",
    "Alaró, Mallorca",
    "Sóller, Mallorca",
    "port de Sóller, Mallorca",
    "Pollença, Mallorca",
    "Deià, Mallorca",
    "Inca, Mallorca",
    "Santa Maria del Camí, Mallorca",
    "Binissalem, Mallorca",
    "Santa Eugenia, Mallorca",
    "Santanyí, Mallorca",
    "Inca, Mallorca",
    "Sencelles, Mallorca",
    "Esporles, Mallorca",
    "Sineu, Mallorca",
    "Port de Andratx, Mallorca",
    "Alcúdia, Mallorca",
    "Can Picafort, Mallorca",
    "Port de pollença, Mallorca",
    "Santa ponça, Mallorca",
    "palmanova, Mallorca",
    "portals nous, Mallorca",
    "port de andratx, Mallorca",
    "Bunyola, Mallorca",
    "Orient, Mallorca",
    "Ma-10, Mallorca"
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
