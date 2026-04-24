import pytesseract
import pandas as pd
import os
import re
import warnings
warnings.filterwarnings("ignore")
from PIL import Image
from pdf2image import convert_from_path

TESSERACT_PATH = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
POPPLER_PATH = r"C:\Users\hp\Downloads\Poppler\poppler-25.12.0\Library\bin"
REPORTS_DIR    = "data/reports"
OUTPUT_DIR     = "data/processed"
OUTPUT_FILE    = f"{OUTPUT_DIR}/kpis_ocr.csv"
KPI_FILE       = "data/processed/kpis.xlsx"
os.makedirs(OUTPUT_DIR, exist_ok=True)

pytesseract.pytesseract.tesseract_cmd = TESSERACT_PATH

PDFS_SCANNÉS = [
    "Aditya_Birla_Fashion_and_Retail_Ltd_2024.pdf",
    "Aditya_Birla_Fashion_and_Retail_Ltd_2025.pdf",
    "Arezzo_Industria_e_Comercio_SA_2022.pdf",
    "Aritzia_2025.pdf",
    "Eclat_Textile_Co_Ltd_2024.pdf",
    "Gap_Inc._2025.pdf",
]

UNITS_PATTERN = r'(mt\s?co2|tco2|tonnes?|tons?|mwh|gwh|kwh|m3|liters?|litres?|%|million|billion|usd|\$|employees?)'


def charger_kpis_officiels(kpi_file):
    df = pd.read_excel(kpi_file)
    prioritaires = df[df["score"].isin(["A+ - Critical", "A - High"])].copy()
    kpis = {}
    for _, row in prioritaires.iterrows():
        kpi_name = str(row["kpi_name"]).strip()
        kpis[kpi_name] = {
            "keywords"   : [kpi_name.lower()],
            "topic"      : row["topic"],
            "topic_fr"   : row["topic_fr"],
            "score"      : row["score"],
            "topic_score": row["topic_score"],
        }
    print(f"{len(kpis)} KPIs officiels chargés")
    return kpis


def pdf_vers_images(chemin_pdf):
    try:
        images = convert_from_path(
            chemin_pdf,
            dpi=150,              # résolution haute pour meilleure OCR(300 dpi peut etre trop lourd)
            poppler_path=POPPLER_PATH
        )
        print(f"  {len(images)} pages converties en images")
        return images
    except Exception as e:
        print(f" Erreur conversion : {e}")
        return []


def ocr_image(image, num_page):
    try:
        texte = pytesseract.image_to_string(
            image,
            lang="eng",
            config="--psm 6"   
        )
        return texte.lower()
    except Exception as e:
        print(f" Erreur OCR page {num_page} : {e}")
        return ""


def chercher_kpis_dans_texte(texte, num_page, kpis_officiels):
    resultats = []

    for kpi_name, kpi_info in kpis_officiels.items():
        mots_cles = " ".join(kpi_info["keywords"][0].split()[:4])

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


def traiter_pdf_scanné(chemin_pdf, company, sector, industry, year, kpis_officiels):
    print(f"\n OCR : {os.path.basename(chemin_pdf)}")

    images = pdf_vers_images(chemin_pdf)
    if not images:
        return []

    tous_resultats = []

    for num_page, image in enumerate(images, 1):
        print(f" OCR page {num_page}/{len(images)}", end="\r")

        texte = ocr_image(image, num_page)
        if not texte:
            continue

        resultats = chercher_kpis_dans_texte(texte, num_page, kpis_officiels)

        for r in resultats:
            r.update({
                "Company Name": company,
                "Sector"      : sector,
                "Industry"    : industry,
                "Year"        : year,
                "Source File" : os.path.basename(chemin_pdf),
                "Method"      : "OCR"
            })
            tous_resultats.append(r)

    seen = set()
    propres = []
    for r in tous_resultats:
        key = (r["kpi_name"], r["value"])
        if key not in seen:
            seen.add(key)
            propres.append(r)

    return propres


def sauvegarder(kpis):
    if not kpis:
        return
    df     = pd.DataFrame(kpis)
    header = not os.path.exists(OUTPUT_FILE)
    df.to_csv(OUTPUT_FILE, mode="a", index=False, header=header)


if __name__ == "__main__":

    kpis_officiels = charger_kpis_officiels(KPI_FILE)
    total_kpis = 0

    for sector in os.listdir(REPORTS_DIR):
        sector_path = os.path.join(REPORTS_DIR, sector)
        if not os.path.isdir(sector_path):
            continue

        for industry in os.listdir(sector_path):
            industry_path = os.path.join(sector_path, industry)
            if not os.path.isdir(industry_path):
                continue

            for filename in os.listdir(industry_path):
                if filename not in PDFS_SCANNÉS:
                    continue

                parts   = filename.replace(".pdf", "").rsplit("_", 1)
                company = parts[0].replace("_", " ") if len(parts) > 1 else filename
                year    = parts[1] if len(parts) > 1 else "unknown"
                chemin  = os.path.join(industry_path, filename)

                kpis = traiter_pdf_scanné(
                    chemin, company, sector, industry, year, kpis_officiels
                )

                if kpis:
                    sauvegarder(kpis)
                    total_kpis += len(kpis)
                    print(f" {len(kpis)} KPIs extraits par OCR")
                else:
                    print(f" Aucun KPI trouvé")

    print(f"""
  OCR terminé
  📊 KPIs extraits par OCR : {total_kpis}
  💾 Sauvegardé : {OUTPUT_FILE}
    """)