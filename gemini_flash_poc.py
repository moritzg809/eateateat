import time
from dotenv import load_dotenv
from google import genai
from google.genai import types

load_dotenv()
client = genai.Client()

PROMPT = """
Schreibe einen hochwertigen deutschen Restaurantartikel über das Restaurant Miceli in Selva, Mallorca.

Recherchiere mit Google Search:
- Die Köchin Marga Coll: Biografie, Ausbildung, Küchenstil, Philosophie
- Das Restaurant selbst: Geschichte, Atmosphäre, Lage, Auszeichnungen (Bib Gourmand)
- Die Küche: typische Gerichte, Marktküche-Konzept, mallorquinische Tradition
- Praktische Infos: Preise, Reservierung, Öffnungszeiten
- Was das Restaurant für deutschsprachige Mallorca-Reisende besonders macht

Zielgruppe: deutschsprachige Reisende, die authentische mallorquinische Küche abseits des Massentourismus suchen.
Format: journalistischer Restaurantartikel mit Einleitung, mehreren Abschnitten und Fazit.
"""

print("Starte Gemini 3 Flash mit Google Search Grounding...")
start = time.time()

response = client.models.generate_content(
    model="gemini-3-flash-preview",
    contents=PROMPT,
    config=types.GenerateContentConfig(
        tools=[types.Tool(google_search=types.GoogleSearch())],
    ),
)

elapsed = time.time() - start
print(f"Fertig in {elapsed:.1f}s\n")

report = response.text
usage = response.usage_metadata

print("="*60)
print(report)
print("="*60)
print(f"\nToken-Usage:")
print(f"  Input:  {usage.prompt_token_count:,}")
print(f"  Output: {usage.candidates_token_count:,}")
print(f"  Cache:  {getattr(usage, 'cached_content_token_count', 0):,}")

with open("miceli_flash.md", "w") as f:
    f.write(report)
print("\nArtikel gespeichert in miceli_flash.md")
