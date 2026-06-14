
"""
etl_pipeline.py - Data Warehouse COVID-19 (UNICAL)
Studentessa: Chiara Costantino - 277081

ETL (Fase 1 + Fase 2) che salva i dati in MySQL:
  - schema_reconciled.sql  (database: covid_reconciled)
  - schema_dw.sql          (database: covid_dw)
  - le strategie di pulizia dei notebook L1-L3 e il design del DW di L4-L5.

Pipeline:
  EXTRACT    legge il file OWID originale (Excel/CSV).
  TRANSFORM  pulisce i dati come in L3 (toglie gli aggregati OWID_, mette a 0
             le misure di flusso mancanti, forward-fill delle cumulative + zeri
             iniziali, deduplica), poi costruisce le tabelle riconciliate e
             quelle a stella (Dim_Time, Dim_Location_Context, Fact).
  LOAD       scrive le 3 tabelle riconciliate in covid_reconciled e le 3 tabelle
             del DW in covid_dw, usando SQLAlchemy DataFrame.to_sql().

Uso:
  pip install pandas sqlalchemy pymysql openpyxl
  python etl_pipeline.py --excel owid-covid-data-2.xlsx \
         --host 127.0.0.1 --user root --password "" --create-schema
"""
import argparse
import os
import urllib.request

import pandas as pd
from sqlalchemy import create_engine, text

# Liste di colonne: combaciano con schema_reconciled.sql / schema_dw.sql
LOCATION_COLS = [
    "iso_code", "location", "continent", "population", "population_density",
    "gdp_per_capita", "median_age", "hospital_beds_per_thousand",
    "human_development_index",
]
EPIDEMIC_COLS = ["iso_code", "date", "new_cases", "total_cases",
                 "new_deaths", "total_deaths"]
PREVENTION_COLS = ["iso_code", "date", "people_vaccinated", "new_tests",
                   "stringency_index"]

# Misure di flusso (giornaliere) e cumulative, per le due tabelle transazionali
FLOW_EPIDEMIC = ["new_cases", "new_deaths"]
CUMULATIVE_EPIDEMIC = ["total_cases", "total_deaths"]
FLOW_PREVENTION = ["new_tests"]
CUMULATIVE_PREVENTION = ["people_vaccinated", "stringency_index"]

DW_DIM_TIME_COLS = ["time_sk", "date", "month", "quarter", "year",
                    "day_of_week", "is_weekend", "days_since_start"]
DW_DIM_LOC_COLS = ["location_sk"] + LOCATION_COLS
DW_FACT_COLS = ["fact_sk", "location_sk", "time_sk", "new_cases", "total_cases",
                "new_deaths", "total_deaths", "new_tests", "stringency_index",
                "people_vaccinated"]

# Colonne grezze che servono davvero (unione di tutte quelle usate sopra)
NEEDED_RAW = sorted(set(
    LOCATION_COLS + ["date", "new_cases", "total_cases", "new_deaths",
                     "total_deaths", "people_vaccinated", "new_tests",
                     "stringency_index"]
))

# Il file locale del progetto e' preferito; questo URL e' solo un ripiego e
# potrebbe dover essere aggiornato se OWID cambia hosting.
OWID_FALLBACK_URL = "https://covid.ourworldindata.org/data/owid-covid-data.csv"


# ============================= EXTRACT =============================
def extract_raw(path):
    """Legge il file OWID grezzo; se non c'e' in locale, lo scarica."""
    if os.path.exists(path):
        print(f"[extract] reading local file: {path}")
        if path.lower().endswith((".xlsx", ".xls")):
            df = pd.read_excel(path, engine="openpyxl")
        else:
            df = pd.read_csv(path)
    else:
        print(f"[extract] '{path}' not found -> downloading from OWID ...")
        local_csv = "owid-covid-data.csv"
        urllib.request.urlretrieve(OWID_FALLBACK_URL, local_csv)
        df = pd.read_csv(local_csv)

    # Tengo solo le colonne che mi servono e converto la colonna data
    keep = [c for c in NEEDED_RAW if c in df.columns]
    df = df[keep].copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    print(f"[extract] raw rows: {len(df):,} x {df.shape[1]} cols")
    return df


# ============ TRANSFORM - pulizia (come L3) + tabelle riconciliate ============
def _clean_block(df, flow_cols, cumulative_cols, clip_cols):
    """Applica la pulizia di L3 a un blocco transazionale (gia' senza OWID_)."""
    df = df.copy()
    # valori negativi (correzioni retroattive di OWID) -> 0
    for c in clip_cols:
        if c in df.columns:
            df.loc[df[c] < 0, c] = 0.0
    # misure di flusso: un giorno mancante = nessun nuovo evento -> 0
    for c in flow_cols:
        if c in df.columns:
            df[c] = df[c].fillna(0.0)
    # cumulative: forward-fill per paese, poi i NaN iniziali diventano 0
    for c in cumulative_cols:
        if c in df.columns:
            df[c] = df.groupby("iso_code")[c].ffill()
            df[c] = df[c].fillna(0.0)
    # tolgo i duplicati sulla chiave composta (iso_code, date)
    df = df.drop_duplicates(subset=["iso_code", "date"], keep="first")
    return df.reset_index(drop=True)


def build_reconciled(raw):
    """Restituisce le tre tabelle riconciliate gia' pulite."""
    # tolgo le righe aggregate OWID_* (non sono paesi reali)
    national = raw[~raw["iso_code"].astype(str).str.startswith("OWID_")].copy()

    locations = (national[LOCATION_COLS]
                 .drop_duplicates(subset="iso_code")
                 .dropna(subset=["iso_code", "location"])
                 .reset_index(drop=True))
    locations["population"] = locations["population"].fillna(0).round().astype("int64")

    epidemic = _clean_block(
        national[EPIDEMIC_COLS],
        flow_cols=FLOW_EPIDEMIC,
        cumulative_cols=CUMULATIVE_EPIDEMIC,
        clip_cols=FLOW_EPIDEMIC + CUMULATIVE_EPIDEMIC,
    )[EPIDEMIC_COLS]

    prevention = _clean_block(
        national[PREVENTION_COLS],
        flow_cols=FLOW_PREVENTION,
        cumulative_cols=CUMULATIVE_PREVENTION,
        clip_cols=FLOW_PREVENTION,
    )[PREVENTION_COLS]

    # converto le misure in interi (tipi dello schema: INT / BIGINT)
    for c in ["new_cases", "total_cases", "new_deaths", "total_deaths"]:
        epidemic[c] = epidemic[c].round().astype("int64")
    for c in ["people_vaccinated", "new_tests"]:
        prevention[c] = prevention[c].round().astype("int64")
    prevention["stringency_index"] = prevention["stringency_index"].round(3)

    print(f"[transform] Locations={len(locations):,}  "
          f"Epidemic_Trend={len(epidemic):,}  "
          f"Prevention_Measures={len(prevention):,}")
    return locations, epidemic, prevention


def build_dw(locations, epidemic, prevention):
    """Costruisce le tre tabelle a stella con chiavi surrogate e lookup delle FK."""
    # ---- Dim_Time ----
    all_dates = pd.date_range(epidemic["date"].min(),
                              epidemic["date"].max(), freq="D")
    start = pd.Timestamp("2020-01-01")
    dim_time = pd.DataFrame({
        "time_sk": range(1, len(all_dates) + 1),
        "date": all_dates,
        "month": all_dates.to_period("M").astype(str),
        "quarter": all_dates.to_period("Q").astype(str),
        "year": all_dates.year,
        "day_of_week": all_dates.dayofweek,
        "is_weekend": all_dates.dayofweek >= 5,
        "days_since_start": (all_dates - start).days,
    })

    # ---- Dim_Location_Context: SK su iso_code ----
    dim_loc = locations.copy().reset_index(drop=True)
    dim_loc.insert(0, "location_sk", range(1, len(dim_loc) + 1))
    dim_loc = dim_loc[DW_DIM_LOC_COLS]

    # ---- Fact_Covid_Trend: prendo le colonne dello schema da ogni sorgente e unisco ----
    ep = epidemic[EPIDEMIC_COLS]
    pre = prevention[PREVENTION_COLS]              # new_tests arriva da qui
    fact = ep.merge(pre, on=["iso_code", "date"], how="left")

    fact = fact.merge(dim_loc[["iso_code", "location_sk"]], on="iso_code", how="left")
    fact["date"] = pd.to_datetime(fact["date"])
    fact = fact.merge(dim_time[["date", "time_sk"]], on="date", how="left")

    # controllo orfani: integrita' referenziale prima del caricamento
    orphans = fact[fact[["location_sk", "time_sk"]].isnull().any(axis=1)]
    if len(orphans):
        raise ValueError(f"FK validation failed: {len(orphans)} orphan fact rows")

    fact["location_sk"] = fact["location_sk"].astype("int64")
    fact["time_sk"] = fact["time_sk"].astype("int64")
    for c in ["new_tests", "people_vaccinated"]:
        fact[c] = fact[c].fillna(0).round().astype("int64")
    fact["stringency_index"] = fact["stringency_index"].fillna(0).round(3)
    fact.insert(0, "fact_sk", range(1, len(fact) + 1))
    fact = fact[DW_FACT_COLS]

    # Dim_Time.date e' VARCHAR(10) nello schema -> la salvo come 'YYYY-MM-DD'
    dim_time["date"] = dim_time["date"].dt.strftime("%Y-%m-%d")
    dim_time = dim_time[DW_DIM_TIME_COLS]

    print(f"[transform] Dim_Time={len(dim_time):,}  "
          f"Dim_Location_Context={len(dim_loc):,}  "
          f"Fact_Covid_Trend={len(fact):,}")
    return dim_time, dim_loc, fact


# ===================== LOAD + DDL + integrita' =====================
def run_sql_script(engine, path):
    """Esegue un file .sql con piu' istruzioni (ignora le righe di commento --)."""
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()
    lines = [ln for ln in raw.splitlines() if not ln.strip().startswith("--")]
    statements = [s.strip() for s in "\n".join(lines).split(";") if s.strip()]
    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))
    print(f"[ddl] executed {len(statements)} statements from {os.path.basename(path)}")


def load_table(engine, df, table):
    # Scrivo il DataFrame nella tabella indicata
    df.to_sql(table, engine, if_exists="append", index=False, chunksize=10_000)
    print(f"[load] {table}: {len(df):,} rows -> {engine.url.database}")


def check_integrity(engine):
    # Conto le righe del fatto che non trovano corrispondenza nelle dimensioni
    q_loc = text("SELECT COUNT(*) FROM Fact_Covid_Trend f "
                 "LEFT JOIN Dim_Location_Context d ON f.location_sk=d.location_sk "
                 "WHERE d.location_sk IS NULL")
    q_time = text("SELECT COUNT(*) FROM Fact_Covid_Trend f "
                  "LEFT JOIN Dim_Time t ON f.time_sk=t.time_sk "
                  "WHERE t.time_sk IS NULL")
    with engine.connect() as conn:
        o_loc = conn.execute(q_loc).scalar()
        o_time = conn.execute(q_time).scalar()
    print(f"[check] orphan location_sk={o_loc}  orphan time_sk={o_time}")
    return o_loc == 0 and o_time == 0


# Main
def main():
    # Leggo i parametri da riga di comando (file di input e credenziali MySQL)
    p = argparse.ArgumentParser(
        description="COVID-19 DW - MySQL ETL (Phase 1 reconciled + Phase 2 DW)")
    p.add_argument("--excel", default="owid-covid-data-2.xlsx",
                   help="path to the raw OWID Excel/CSV file")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", default="3306")
    p.add_argument("--user", default="root")
    p.add_argument("--password", default="")
    p.add_argument("--create-schema", action="store_true",
                   help="run schema_reconciled.sql and schema_dw.sql first")
    p.add_argument("--ddl-reconciled", default="schema_reconciled.sql")
    p.add_argument("--ddl-dw", default="schema_dw.sql")
    args = p.parse_args()

    server_url = (f"mysql+pymysql://{args.user}:{args.password}"
                  f"@{args.host}:{args.port}")

    # 0) se richiesto, creo i due database dai file DDL
    if args.create_schema:
        engine_server = create_engine(server_url)
        run_sql_script(engine_server, args.ddl_reconciled)
        run_sql_script(engine_server, args.ddl_dw)

    # 1) EXTRACT + TRANSFORM
    raw = extract_raw(args.excel)
    locations, epidemic, prevention = build_reconciled(raw)
    dim_time, dim_loc, fact = build_dw(locations, epidemic, prevention)

    # 2) LOAD - database riconciliato (Fase 1). Prima Locations .
    eng_recon = create_engine(server_url + "/covid_reconciled")
    load_table(eng_recon, locations, "Locations")
    load_table(eng_recon, epidemic, "Epidemic_Trend")
    load_table(eng_recon, prevention, "Prevention_Measures")

    # 3) LOAD - data warehouse (Fase 2). Prima le dimensioni, poi il fatto.
    eng_dw = create_engine(server_url + "/covid_dw")
    load_table(eng_dw, dim_time, "Dim_Time")
    load_table(eng_dw, dim_loc, "Dim_Location_Context")
    load_table(eng_dw, fact, "Fact_Covid_Trend")

    # 4) controllo finale di integrita' referenziale
    ok = check_integrity(eng_dw)
    print("\nETL completed." + ("" if ok else "  WARNING: orphan keys found!"))


if __name__ == "__main__":
    main()
