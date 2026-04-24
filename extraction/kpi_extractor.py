import pdfplumber
import pandas as pd
import os
import re
import warnings
warnings.filterwarnings("ignore")

REPORTS_DIR = "data/reports"
OUTPUT_DIR  = "data/processed"
OUTPUT_FILE = f"{OUTPUT_DIR}/kpis_extracted.csv"
KPI_FILE    = "data/processed/kpis.xlsx"   
os.makedirs(OUTPUT_DIR, exist_ok=True)

UNITS_PATTERN = r'(mt\s?co2|tco2|tonnes?|tons?|mwh|gwh|kwh|m3|liters?|litres?|%|million|billion|usd|\$|employees?|hectares?|km2)'


def charger_kpis_officiels(kpi_file):
    
    df = pd.read_excel(kpi_file)

    prioritaires = df[df["score"].isin(["A+ - Critical", "A - High"])].copy()

    kpis = {}
    for _, row in prioritaires.iterrows():
        kpi_name = str(row["kpi_name"]).strip()
        mots = kpi_name.lower()
        kpis[kpi_name] = {
            "keywords"   : [mots],
            "topic"      : row["topic"],
            "topic_fr"   : row["topic_fr"],
            "score"      : row["score"],
            "topic_score": row["topic_score"],
            "source"     : row["source"],
        }

    print(f" {len(kpis)} KPIs officiels chargés (A+ Critical + A High)")
    return kpis


def extraire_texte_pdf(chemin_pdf):
    texte_complet = []
    try:
        with pdfplumber.open(chemin_pdf) as pdf:
            for num_page, page in enumerate(pdf.pages, 1):
                texte = page.extract_text()
                if texte:
                    texte_complet.append({
                        "page" : num_page,
                        "texte": texte.lower()
                    })
    except Exception as e:
        print(f"  Erreur lecture PDF : {e}")
    return texte_complet


def chercher_kpi_dans_texte(texte, num_page, kpi_name, kpi_info):
    resultats = []

    for keyword in kpi_info["keywords"]:
        mots_cles = " ".join(keyword.split()[:4])
        if mots_cles in texte:
            lignes = texte.split('\n')
            for ligne in lignes:
                if mots_cles in ligne:
                    nombres = re.findall(r'\b\d[\d,\.]*\b', ligne)
                    unites  = re.findall(UNITS_PATTERN, ligne, re.IGNORECASE)
                    if nombres:
                        resultats.append({
                            "kpi_name"   : kpi_name,
                            "topic"      : kpi_info["topic"],
                            "topic_fr"   : kpi_info["topic_fr"],
                            "score"      : kpi_info["score"],
                            "topic_score": kpi_info["topic_score"],
                            "value"      : nombres[0],
                            "unit"       : unites[0] if unites else "unknown",
                            "page"       : num_page,
                            "context"    : ligne.strip()[:200],
                        })
    return resultats


def extraire_kpis_pdf(chemin_pdf, company, sector, industry, year, kpis_officiels):
    pages = extraire_texte_pdf(chemin_pdf)
    if not pages:
        return []

    tous_resultats = []
    for page_data in pages:
        for kpi_name, kpi_info in kpis_officiels.items():
            resultats = chercher_kpi_dans_texte(
                page_data["texte"],
                page_data["page"],
                kpi_name,
                kpi_info
            )
            for r in resultats:
                r.update({
                    "Company Name": company,
                    "Sector"      : sector,
                    "Industry"    : industry,
                    "Year"        : year,
                    "Source File" : os.path.basename(chemin_pdf),
                })
                tous_resultats.append(r)

    seen = set()
    resultats_propres = []
    for r in tous_resultats:
        key = (r["kpi_name"], r["value"])
        if key not in seen:
            seen.add(key)
            resultats_propres.append(r)

    return resultats_propres


def sauvegarder(all_kpis):
    if not all_kpis:
        return
    df     = pd.DataFrame(all_kpis)
    cols   = ["Company Name", "Sector", "Industry", "Year",
              "kpi_name", "topic", "topic_fr", "score", "topic_score",
              "value", "unit", "page", "context", "Source File"]
    df     = df[[c for c in cols if c in df.columns]]
    header = not os.path.exists(OUTPUT_FILE)
    df.to_csv(OUTPUT_FILE, mode="a", index=False, header=header)


if __name__ == "__main__":

    kpis_officiels = charger_kpis_officiels(KPI_FILE)

    deja_traites = set()
    if os.path.exists(OUTPUT_FILE):
        df_existant  = pd.read_csv(OUTPUT_FILE)
        deja_traites = set(df_existant["Source File"].unique())
        print(f" {len(deja_traites)} fichiers déjà traités — reprise...\n")
    else:
        print("Démarrage extraction KPIs officiels...\n")

    fichiers_traites = 0
    fichiers_skipped = 0
    fichiers_erreur  = 0

    for sector in os.listdir(REPORTS_DIR):
        sector_path = os.path.join(REPORTS_DIR, sector)
        if not os.path.isdir(sector_path):
            continue

        for industry in os.listdir(sector_path):
            industry_path = os.path.join(sector_path, industry)
            if not os.path.isdir(industry_path):
                continue

            for filename in os.listdir(industry_path):
                if not filename.endswith(".pdf"):
                    continue

                if filename in deja_traites:
                    fichiers_skipped += 1
                    continue

                parts   = filename.replace(".pdf", "").rsplit("_", 1)
                company = parts[0].replace("_", " ") if len(parts) > 1 else filename
                year    = parts[1] if len(parts) > 1 else "unknown"
                chemin  = os.path.join(industry_path, filename)

                print(f"  {filename}")
                kpis = extraire_kpis_pdf(
                    chemin, company, sector, industry, year, kpis_officiels
                )

                if kpis:
                    sauvegarder(kpis)
                    fichiers_traites += 1
                    print(f"  {len(kpis)} KPIs extraits")
                else:
                    fichiers_erreur += 1
                    print(f"  Aucun KPI trouvé")

    total = len(pd.read_csv(OUTPUT_FILE)) if os.path.exists(OUTPUT_FILE) else 0
    print(f"""
  Extraction terminée
  ⏭️  Déjà traités     : {fichiers_skipped}
  ✅ Nouveaux traités  : {fichiers_traites}
  ❌ Sans résultat     : {fichiers_erreur}
  📊 Total KPIs en CSV : {total}
    """)



  