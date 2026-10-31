import os
import re
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# ---------- 1. Load config from .env ----------
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "")
DB_PORT = int(os.getenv("DB_PORT", ""))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
CSV_FOLDER = os.getenv("CSV_FOLDER", "")          # folder containing csv files
IF_EXISTS = os.getenv("IF_EXISTS", "replace")         # replace | append | fail

if not all([DB_USER, DB_PASSWORD, DB_NAME]):
    raise SystemExit("Missing DB_USER / DB_PASSWORD / DB_NAME in .env file")


# ---------- 2. Helpers ----------
def clean_name(name: str) -> str:
    """'Order Details (2024).csv' -> 'order_details_2024'"""
    name = Path(name).stem.lower()
    name = re.sub(r"[^a-z0-9]+", "_", name).strip("_")
    return name


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """'Order Date' -> 'order_date' so MySQL doesn't need backticks."""
    df.columns = [re.sub(r"[^a-z0-9]+", "_", c.strip().lower()).strip("_") for c in df.columns]
    return df


def read_csv_safe(path: Path) -> pd.DataFrame:
    """Try utf-8 first, fall back to latin1 (common for Excel-exported CSVs)."""
    try:
        return pd.read_csv(path, encoding="utf-8")
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="latin1")


# ---------- 3. Create database if missing ----------
server_url = URL.create("mysql+pymysql", DB_USER, DB_PASSWORD, DB_HOST, DB_PORT)
with create_engine(server_url).connect() as conn:
    conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}`"))
    conn.commit()

db_url = URL.create("mysql+pymysql", DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)
engine = create_engine(db_url)


# ---------- 4. Import every CSV ----------
csv_files = sorted(Path(CSV_FOLDER).glob("*.csv"))
if not csv_files:
    raise SystemExit(f"No CSV files found in '{CSV_FOLDER}'")

success, failed = [], []

for file in csv_files:
    table = clean_name(file.name)
    try:
        df = clean_columns(read_csv_safe(file))
        df.to_sql(table, engine, if_exists=IF_EXISTS, index=False, chunksize=5000)
        print(f"[OK]   {file.name:35} -> {table:25} ({len(df):,} rows)")
        success.append(table)
    except Exception as e:
        print(f"[FAIL] {file.name:35} -> {e}")
        failed.append(file.name)

# ---------- 5. Summary ----------
print("\n----- Summary -----")
print(f"Imported : {len(success)} -> {success}")
print(f"Failed   : {len(failed)} -> {failed}")