import time
from dotenv import load_dotenv
from google import genai

load_dotenv()
client = genai.Client()

PROMPT = """
Schreibe einen hochwertigen deutschen Restaurantartikel über das Restaurant Miceli in Selva, Mallorca.

Recherchiere:
- Die Köchin Marga Coll: Biografie, Ausbildung, Küchenstil, Philosophie
- Das Restaurant selbst: Geschichte, Atmosphäre, Lage, Auszeichnungen (Bib Gourmand)
- Die Küche: typische Gerichte, Marktküche-Konzept, mallorquinische Tradition
- Praktische Infos: Preise, Reservierung, Öffnungszeiten
- Was das Restaurant für deutschsprachige Mallorca-Reisende besonders macht

Zielgruppe: deutschsprachige Reisende, die authentische mallorquinische Küche abseits des Massentourismus suchen.
Format: journalistischer Restaurantartikel mit Einleitung, mehreren Abschnitten und Fazit.
"""

print("Starte Deep Research...")
interaction = client.interactions.create(
    input=PROMPT,
    agent="deep-research-pro-preview-12-2025",
    background=True,
)

print(f"Task ID: {interaction.id}")
print("Warte auf Ergebnis (kann mehrere Minuten dauern)...\n")

while True:
    interaction = client.interactions.get(interaction.id)
    print(f"Status: {interaction.status}")
    if interaction.status == "completed":
        report = interaction.outputs[-1].text
        print("\n" + "="*60)
        print(report)
        print("="*60)
        with open("miceli_deep_research.md", "w") as f:
            f.write(report)
        print("\nArtikel gespeichert in miceli_deep_research.md")
        break
    elif interaction.status == "failed":
        print(f"Fehler: {interaction.error}")
        break
    time.sleep(15)
