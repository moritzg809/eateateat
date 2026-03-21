# =============================================================
# Scraper configuration — EatEatEat multi-city
#
# Add new cities to the CITIES dict below.
# The backward-compat aliases at the bottom keep pipeline.py
# working without any changes (defaults to Mallorca).
# =============================================================

CITIES = {
    "mallorca": {
        "db_id": 1,
        "name": "Mallorca",
        "search_terms": [
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
            "Agroturismen restaurant",
            "Meerview restaurant",
            "Mexican/taco restaurant",
        ],
        "locations": [
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
            "Ma-10, Mallorca",
            "Casco Antiguo Mallorca",
            "La Lonja Mallorca",
            "Molinar Mallorca",
            "El Terreno Mallorca",
            "Passeig del Born Mallorca",
            "Jaime III Mallorca",
            "Cala Major Mallorca",
            "Pere Garau Mallorca",
            "Son Armadams Mallorca",
            "Son Vida Mallorca",
            "Selva, Mallorca",
            "Consell, Mallorca",
            "Biniagual, Mallorca",
        ],
        "min_rating": 4.5,
        "min_rating_count": 100,
        "search_language": "de",
        "search_country": "es",
    },

    # ── Add new cities here ────────────────────────────────────────────────────
    "stuttgart": {
        "db_id": 2,
        "name": "Stuttgart",
        "search_terms": [
            "Restaurant",
            "Weinbar",
            "Brunch",
            "Specialty Coffee",
            "Fine Dining",
            "Bistro",
            "Tapas",
            "Sushi",
            "Steakhouse",
            "Burger",
            "Vegetarisch",
            "Vegan",
            "Italienisch",
            "Asiatisch",
            "Bar",
            "Rooftop Bar",
            "Cocktailbar",
            "Bäckerei Café",
            "Frühstück",
            "Mittagstisch",
        ],
        "locations": [
            "Stuttgart",
            "Stuttgart-Mitte",
            "Stuttgart-West",
            "Stuttgart-Süd",
            "Stuttgart-Nord",
            "Stuttgart-Ost",
            "Bohnenviertel Stuttgart",
            "Leonhardsviertel Stuttgart",
            "Hospitalviertel Stuttgart",
            "Marktplatz Stuttgart",
            "Königstraße Stuttgart",
            "Calwer Straße Stuttgart",
            "Rotebühlplatz Stuttgart",
            "Heusteigviertel Stuttgart",
            "Stuttgarter Weinsteige",
        ],
        "min_rating": 4.3,
        "min_rating_count": 50,
        "search_language": "de",
        "search_country": "de",
    },

    # ── Template for more cities ───────────────────────────────────────────────
    # "muenchen": {
    #     "db_id": 3,
    #     "name": "München",
    #     "search_terms": [ "Restaurant", ... ],
    #     "locations": [ "München", "Schwabing", ... ],
    #     "min_rating": 4.3,
    #     "min_rating_count": 50,
    #     "search_language": "de",
    #     "search_country": "de",
    # },
}

# ── Backward-compat aliases ────────────────────────────────────────────────────
# pipeline.py imports these by name — no changes needed there.

SEARCH_TERMS     = CITIES["mallorca"]["search_terms"]
LOCATIONS        = CITIES["mallorca"]["locations"]
MIN_RATING       = CITIES["mallorca"]["min_rating"]
MIN_RATING_COUNT = CITIES["mallorca"]["min_rating_count"]

# ── API settings (global, not per-city) ───────────────────────────────────────

SERPER_MAPS_URL   = "https://google.serper.dev/maps"
SERPER_PLACES_URL = "https://google.serper.dev/places"

SEARCH_LANGUAGE  = CITIES["mallorca"]["search_language"]
SEARCH_COUNTRY   = CITIES["mallorca"]["search_country"]
RESULTS_PER_CALL = 20    # max supported by Serper Maps
