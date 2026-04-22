import requests
import pandas as pd
import os

OUTPUT_DIR = "data/processed"
os.makedirs(OUTPUT_DIR, exist_ok=True)

API_URL = "https://owaeaasu09.execute-api.us-west-2.amazonaws.com/prod/navigator-data/reports?locale=en-gb"

headers = {
    "User-Agent"     : "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept"         : "application/json",
    "Referer"        : "https://navigator.sasb.ifrs.org/reporters",
    "Accept-Language": "en-GB,en;q=0.9",
    "Origin"         : "https://navigator.sasb.ifrs.org",
}

print("Connexion à l'API SASB...")
response = requests.get(API_URL, headers=headers)
print(f"Status : {response.status_code}")

data = response.json()
print(f" {len(data)} compagnies reçues")

rows = []
for item in data:
    rows.append({
        "Company Name"    : item.get("name", ""),
        "Industry"        : item.get("sics_industry__c", ""),
        "Sector"          : item.get("sics_sector__c", ""),
        "Country"         : item.get("country_of_domicile__c", ""),
        "Type of Document": item.get("source_type__c", ""),
        "Report Period"   : item.get("report_period__c", ""),
        "Year"            : item.get("year", ""),
        "Report Link"     : item.get("url__c", ""),
    })

df = pd.DataFrame(rows)

df = df.drop_duplicates()
df = df[df["Report Link"] != ""]   # garder seulement les lignes avec un lien
df = df.sort_values(["Sector", "Industry", "Company Name"])
df = df.reset_index(drop=True)

output_path = f"{OUTPUT_DIR}/sasb_final.csv"
df.to_csv(output_path, index=False)

print(f" {len(df)} compagnies avec liens sauvegardées dans {output_path}")
print(f"\nAperçu :")
print(df.head(10).to_string())
print(f"\nSecteurs disponibles :")
print(df["Sector"].value_counts().to_string())