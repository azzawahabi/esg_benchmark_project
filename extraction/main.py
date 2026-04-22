import pandas as pd
from pdf_parser import extract_text_from_pdf
from kpi_extractor import load_kpi_keywords, find_kpis_in_text

# 📄 Chemins des fichiers
pdf_path = "AAR_Corp_2024.pdf"
kpi_file = "esg_kpis.csv"

# 🔍 Extraire le texte du PDF
texte = extract_text_from_pdf(pdf_path)

# 📥 Charger les KPI depuis le fichier CSV
kpi_list = load_kpi_keywords(kpi_file)

# 🔎 Rechercher tous les KPI dans le texte avec scores (doublons inclus)
trouves = find_kpis_in_text(texte, kpi_list)

# 🖨️ Afficher les résultats
print("\nKPI trouvés :")
for kpi, _, score in trouves:
    print(f"- {kpi} (score={score:.2f})")

# 💾 Exporter dans un fichier CSV 
df = pd.DataFrame(trouves, columns=["KPI", "Contexte", "Score"])
df.to_csv("kpi_extraits.csv", index=False)
print("\n Résultat exporté dans 'kpi_extraits.csv'")