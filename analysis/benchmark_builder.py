import pandas as pd
import numpy as np
import os

KPI_CSV     = "data/processed/kpis_extracted.csv"
OUTPUT_DIR  = "data/processed"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def charger_kpis(kpi_csv):
    df = pd.read_csv(kpi_csv)
    print(f" {len(df)} KPIs chargés")
    print(f" Colonnes : {list(df.columns)}")
    print(f" Compagnies : {df['Company Name'].nunique()}")
    print(f" KPI types : {df['kpi_name'].nunique()}")
    return df


def nettoyer_valeurs(df):
    
    def convertir(val):
        try:
            val_propre = str(val).replace(",", "").replace(" ", "")
            return float(val_propre)
        except:
            return np.nan

    df["value_num"] = df["value"].apply(convertir)

    df = df[df["value_num"] > 0]
    df = df[df["value_num"] < 1_000_000_000]  

    avant = len(df)
    df = df.dropna(subset=["value_num"])
    print(f" {len(df)} valeurs numériques valides ({avant - len(df)} supprimées)")
    return df


def construire_benchmark(df):
    
    resultats = []

    groupes = df.groupby(["Sector", "Industry", "kpi_name"])

    for (sector, industry, kpi_name), groupe in groupes:

        if groupe["Company Name"].nunique() < 2:
            continue

        valeurs = groupe["value_num"]

        moyenne = valeurs.mean()
        minimum = valeurs.min()
        maximum = valeurs.max()
        mediane = valeurs.median()
        nb_companies = groupe["Company Name"].nunique()

        idx_leader  = groupe["value_num"].idxmin()
        idx_laggard = groupe["value_num"].idxmax()

        leader  = groupe.loc[idx_leader,  "Company Name"]
        laggard = groupe.loc[idx_laggard, "Company Name"]

        topic      = groupe["topic"].iloc[0]      if "topic"      in groupe.columns else ""
        topic_fr   = groupe["topic_fr"].iloc[0]   if "topic_fr"   in groupe.columns else ""
        score      = groupe["score"].iloc[0]       if "score"      in groupe.columns else ""
        topic_score= groupe["topic_score"].iloc[0] if "topic_score"in groupe.columns else ""
        unite      = groupe["unit"].mode()[0] if len(groupe["unit"].mode()) > 0 else "unknown"

        resultats.append({
            "Sector"          : sector,
            "Industry"        : industry,
            "KPI Name"        : kpi_name,
            "Topic"           : topic,
            "Topic FR"        : topic_fr,
            "Score"           : score,
            "Topic Score"     : topic_score,
            "Unit"            : unite,
            "Nb Companies"    : nb_companies,
            "Average"         : round(moyenne, 2),
            "Median"          : round(mediane, 2),
            "Min"             : round(minimum, 2),
            "Max"             : round(maximum, 2),
            "Leader"          : leader,
            "Laggard"         : laggard,
        })

    df_benchmark = pd.DataFrame(resultats)
    print(f" {len(df_benchmark)} benchmarks construits")
    return df_benchmark


def construire_scorecard(df):
    scorecard = []

    for company, groupe in df.groupby("Company Name"):
        nb_critical = 0
        nb_high     = 0
        nb_total    = len(groupe)

        if "score" in groupe.columns:
            nb_critical = len(groupe[groupe["score"] == "A+ - Critical"])
            nb_high     = len(groupe[groupe["score"] == "A - High"])

        score_transparence = round(
            (nb_critical + nb_high) / nb_total * 100, 1
        ) if nb_total > 0 else 0

        scorecard.append({
            "Company Name"       : company,
            "Sector"             : groupe["Sector"].iloc[0],
            "Industry"           : groupe["Industry"].iloc[0],
            "Total KPIs"         : nb_total,
            "Critical KPIs (A+)" : nb_critical,
            "High KPIs (A)"      : nb_high,
            "Transparency Score" : score_transparence,
        })

    df_scorecard = pd.DataFrame(scorecard)
    df_scorecard = df_scorecard.sort_values(
        "Transparency Score", ascending=False
    ).reset_index(drop=True)

    print(f" Scorecard construite pour {len(df_scorecard)} compagnies")
    return df_scorecard


if __name__ == "__main__":

    print("=" * 50)
    print("BENCHMARK ESG — Construction")
    print("=" * 50)

    df = charger_kpis(KPI_CSV)

    df = nettoyer_valeurs(df)

    df_benchmark = construire_benchmark(df)

    df_scorecard = construire_scorecard(df)

    benchmark_path = f"{OUTPUT_DIR}/benchmark.csv"
    scorecard_path = f"{OUTPUT_DIR}/scorecard.csv"

    df_benchmark.to_csv(benchmark_path, index=False)
    df_scorecard.to_csv(scorecard_path, index=False)

    print(f"""
  Benchmark terminé !
  📊 Benchmarks    : {len(df_benchmark)} lignes
  🏆 Scorecard     : {len(df_scorecard)} compagnies
  💾 benchmark.csv : {benchmark_path}
  💾 scorecard.csv : {scorecard_path}
    """)

    print("TOP 10 SCORECARD (entreprises les plus transparentes) :")
    print(df_scorecard.head(10).to_string())

    print("\nAPERÇU BENCHMARK :")
    print(df_benchmark.head(10).to_string())