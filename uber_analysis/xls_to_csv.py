"""
Import .xls files into MySQL tables.
Requires: pandas, sqlalchemy, mysql-connector-python, xlrd, python-dotenv
Install:  pip install pandas sqlalchemy mysql-connector-python xlrd python-dotenv
"""

import os
import sys
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

# ---------- CONFIG (set these in a .env file next to this script) ----------
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Map each source .xls file -> destination MySQL table
FILE_TABLE_MAP = {
    os.getenv("XLS_LOCATION"): "location",
    os.getenv("XLS_TRIP_DETAILS"): "trip_details",
}
# -----------------------------------------------------------------------------


def get_engine():
    """Create a SQLAlchemy engine connected to MySQL."""
    if not all([DB_USER, DB_PASSWORD, DB_NAME]):
        sys.exit("Missing DB_USER / DB_PASSWORD / DB_NAME in .env file.")
    conn_str = f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
    return create_engine(conn_str)


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize column names: lowercase, no spaces."""
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    return df


def load_file_to_table(file_path: str, table_name: str, engine, if_exists: str = "append"):
    """Read one .xls file and load it into the given MySQL table."""
    if not os.path.exists(file_path):
        print(f"[SKIP] File not found: {file_path}")
        return

    df = pd.read_excel(file_path, engine="openpyxl")    # openpyxl handles .xlsx
    df = clean_columns(df)

    df.to_sql(name=table_name, con=engine, if_exists=if_exists, index=False)
    print(f"[OK] {file_path} -> `{table_name}` ({len(df)} rows loaded)")


def main():
    engine = get_engine()
    try:
        for file_path, table_name in FILE_TABLE_MAP.items():
            # if_exists="append" assumes the tables already exist (as you defined via CREATE TABLE).
            # Use "replace" instead only if you want pandas to drop/recreate the table itself.
            load_file_to_table(file_path, table_name, engine, if_exists="append")
    except Exception as e:
        print(f"[ERROR] {e}")
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()