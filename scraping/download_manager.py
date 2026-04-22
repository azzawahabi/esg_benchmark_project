import pandas as pd
import requests
import os
import time
import re
from pathlib import Path

CSV_PATH    = "data/processed/sasb_final.csv"
OUTPUT_DIR  = "data/reports"
SECTEUR     = "Consumer Goods"   
MAX_FICHIERS = 100               
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
}

def nom_fichier_propre(texte):
    """Supprime les caractères interdits dans un nom de fichier."""
    texte = re.sub(r'[\\/*?:"<>|]', "", texte)
    texte = texte.strip().replace(" ", "_")
    return texte[:80]   # max 80 caractères


def deviner_extension(url, content_type):
    url_lower = url.lower()
    if ".pdf"  in url_lower: return ".pdf"
    if ".html" in url_lower or ".htm" in url_lower: return ".html"
    if ".xlsx" in url_lower: return ".xlsx"
    if "pdf"   in content_type: return ".pdf"
    if "html"  in content_type: return ".html"
    return ".pdf"   # par défaut


def telecharger_fichier(url, chemin_dest):
   
    if os.path.exists(chemin_dest):
        return "skip"

    try:
        response = requests.get(
            url,
            headers=headers,
            timeout=30,        
            stream=True,       
            allow_redirects=True
        )

        if response.status_code != 200:
            return f"error_{response.status_code}"

        os.makedirs(os.path.dirname(chemin_dest), exist_ok=True)

        with open(chemin_dest, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        return "success"

    except requests.exceptions.Timeout:
        return "error_timeout"
    except requests.exceptions.ConnectionError:
        return "error_connection"
    except Exception as e:
        return f"error_{str(e)[:50]}"


if __name__ == "__main__":

    df = pd.read_csv(CSV_PATH)
    print(f"✅ CSV chargé : {len(df)} compagnies au total")

    # Filtrer par secteur
    df_secteur = df[df["Sector"] == SECTEUR].copy()
    print(f"✅ Secteur '{SECTEUR}' : {len(df_secteur)} rapports")

    # Limiter pour le test
    df_secteur = df_secteur.head(MAX_FICHIERS)
    print(f"✅ Limite appliquée : {len(df_secteur)} rapports à télécharger\n")

    log = []
    success = 0
    skipped = 0
    errors  = 0

    for idx, row in df_secteur.iterrows():

        company  = nom_fichier_propre(str(row["Company Name"]))
        industry = nom_fichier_propre(str(row["Industry"]))
        sector   = nom_fichier_propre(str(row["Sector"]))
        year     = str(row.get("Year", "unknown"))
        url      = str(row["Report Link"])

        # Structure du dossier : data/reports/Sector/Industry/
        dossier = os.path.join(OUTPUT_DIR, sector, industry)

        ext      = deviner_extension(url, "")
        filename = f"{company}_{year}{ext}"
        chemin   = os.path.join(dossier, filename)

        statut = telecharger_fichier(url, chemin)

        if statut == "success":
            success += 1
            print(f"   [{success+skipped+errors}/{len(df_secteur)}] {company}_{year}{ext}")
        elif statut == "skip":
            skipped += 1
            print(f"  ⏭  [{success+skipped+errors}/{len(df_secteur)}] Déjà téléchargé : {filename}")
        else:
            errors += 1
            print(f"   [{success+skipped+errors}/{len(df_secteur)}] Erreur {statut} : {company}")

        # Log
        log.append({
            "Company Name"   : row["Company Name"],
            "Industry"       : row["Industry"],
            "Sector"         : row["Sector"],
            "Year"           : year,
            "File Name"      : filename,
            "File Path"      : chemin,
            "Download Status": statut,
            "File Type"      : ext.replace(".", "").upper(),
            "Source URL"     : url,
        })

        time.sleep(DELAI)

    df_log = pd.DataFrame(log)
    log_path = "data/processed/download_log.csv"
    df_log.to_csv(log_path, index=False)
