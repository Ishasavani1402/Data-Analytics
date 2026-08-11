import os
import sys
import pandas as pd
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv       # pip install python-dotenv

load_dotenv()

# 1. DATABASE CONNECTION
def create_connection():
    try:
        conn = mysql.connector.connect(
            host     = os.environ.get("DB_HOST",     ""),
            user     = os.environ.get("DB_USER",     ""),
            password = os.environ.get("DB_PASSWORD", ""),
            database = os.environ.get("DB_NAME", "")
        )
        if conn.is_connected():
            print("✅ Connected to MySQL")
        return conn
    except Error as e:
        print(f"❌ Error connecting to MySQL: {e}")
        return None


# 2. MAP PANDAS DTYPE → CORRECT MYSQL TYPE
def get_mysql_type(col, dtype):
    if "int"      in str(dtype):   return "INT"
    if "float"    in str(dtype):   return "FLOAT"
    if "datetime" in str(dtype):   return "DATETIME"
    if "object"   in str(dtype):   return "VARCHAR(255)"
    return "TEXT"



# 3. SAFE NULL HANDLING
def safe_fillna(df):
    num_cols = df.select_dtypes(include=['float64', 'int64']).columns
    str_cols = df.select_dtypes(include=['object']).columns

    df[num_cols] = df[num_cols].where(df[num_cols].notna(), None)
    df[str_cols] = df[str_cols].fillna("")

    return df


# 4. CREATE TABLE FROM CSV SCHEMA
def create_table_from_csv(cursor, table_name, df):
    columns = []
    for col, dtype in zip(df.columns, df.dtypes):
        mysql_type = get_mysql_type(col, dtype)
        columns.append(f"`{col}` {mysql_type}")

    columns_sql = ", ".join(columns)

    cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")

    create_table_query = f"""
        CREATE TABLE `{table_name}` (
            {columns_sql}
        );
    """
    cursor.execute(create_table_query)
    print(f"📌 Table `{table_name}` created.")


# 5. INSERT CSV DATA INTO MYSQL
def insert_data(cursor, conn, table_name, df):
    placeholders = ", ".join(["%s"] * len(df.columns))
    columns      = ", ".join([f"`{col}`" for col in df.columns])
    insert_query = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"

    df = safe_fillna(df)

    total_rows = len(df)
    inserted   = 0

    for i in range(0, total_rows, 1000):
        chunk = df.iloc[i:i + 1000]
        data  = [
            tuple(None if pd.isna(v) else v for v in row)
            for row in chunk.values
        ]
        cursor.executemany(insert_query, data)
        conn.commit()
        inserted += len(chunk)
        print(f"   ↳ Inserted {inserted}/{total_rows} rows...", end="\r")

    print(f"\n🚀 Data inserted into `{table_name}` ({inserted} rows total)")


# 6. LOAD A SINGLE CSV → MYSQL
#    table_name is now passed in explicitly (not guessed from filename)
#    -> avoids problems like "pizza types.csv" (has a space) becoming a bad table name
def load_single_csv_to_mysql(file_path, table_name, conn):
    cursor = conn.cursor()

    print(f"\n📂 Processing: {file_path}  →  table `{table_name}`")

    df = pd.read_csv(file_path)
    print(f"   Shape: {df.shape[0]} rows × {df.shape[1]} columns")

    create_table_from_csv(cursor, table_name, df)
    insert_data(cursor, conn, table_name, df)

    cursor.close()


# 7. MAIN — loop over every CSV listed in .env
if __name__ == "__main__":

    # table_name : env_var_name
    CSV_FILES = {
        "orders":         os.environ.get("CSV_ORDERS"),
        "order_details":  os.environ.get("CSV_ORDER_DETAILS"),
        "pizza_types":    os.environ.get("CSV_PIZZA_TYPES"),
        "pizzas":         os.environ.get("CSV_PIZZAS"),
    }

    connection = create_connection()

    if connection:
        loaded = 0
        for table_name, file_path in CSV_FILES.items():
            if not file_path:
                print(f"⚠️  Skipping `{table_name}` — no path set in .env")
                continue
            if not os.path.exists(file_path):
                print(f"❌ File not found for `{table_name}`: {file_path}")
                continue

            load_single_csv_to_mysql(file_path, table_name, connection)
            loaded += 1

        connection.close()
        print(f"\n🎉 Done! {loaded}/{len(CSV_FILES)} CSVs loaded successfully into MySQL!")