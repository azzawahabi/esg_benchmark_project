import pandas as pd
import re
from rapidfuzz import fuzz


def load_kpi_keywords(file_path):
    df = pd.read_csv(file_path, encoding="latin1", sep=";")  
    print("Colonnes détectées :", df.columns)
    kpi_names = df['kpi_name'].dropna().unique().tolist()

    with open("extracted_kpis.txt", "w", encoding="utf-8") as f:
        for kpi in kpi_names:
            f.write(f"{kpi}\n")

    print(f"{len(kpi_names)} KPI enregistrés dans extracted_kpis.txt")
    return kpi_names

def find_kpis_in_text(text, kpi_list, threshold=85):
    found_kpis = []
    lines = text.split("\n")
    for kpi in kpi_list:
        for line in lines:
            score = fuzz.partial_ratio(kpi.lower(), line.lower())
            if score >= threshold:
                found_kpis.append((kpi, line.strip(), score))
                break  
    return found_kpis
