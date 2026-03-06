import os
import json
import pandas as pd
import mysql.connector
from tqdm import tqdm

# ==========================
# CONFIG
# ==========================

JSON_FOLDER = r"D:\Data Analytics\Yelp Analysis\json"
BATCH_SIZE = 10000

MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "123456",
    "database": "yelp"
}

# ==========================
# MYSQL CONNECTION
# ==========================

def connect_mysql():
    return mysql.connector.connect(**MYSQL_CONFIG)

# ==========================
# CREATE TABLE WITH PANDAS TYPES
# ==========================

def create_table_from_df(cursor, table_name, df):

    dtype_map = {
        "int64": "INT",
        "float64": "DOUBLE",
        "bool": "TINYINT",
        "object": "LONGTEXT"
    }

    columns = []

    for col, dtype in df.dtypes.items():
        mysql_type = dtype_map.get(str(dtype), "TEXT")
        columns.append(f"`{col}` {mysql_type}")

    cols = ", ".join(columns)

    query = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        {cols}
    )
    """

    cursor.execute(query)

# ==========================
# INSERT DATA
# ==========================

def insert_batch(cursor, table_name, df):

    placeholders = ", ".join(["%s"] * len(df.columns))
    columns = ", ".join([f"`{c}`" for c in df.columns])

    query = f"""
    INSERT INTO `{table_name}` ({columns})
    VALUES ({placeholders})
    """

    cursor.executemany(query, df.values.tolist())

# ==========================
# PROCESS JSON FILE
# ==========================

def process_json(file_path, table_name):

    print(f"\nProcessing {table_name}")

    conn = connect_mysql()
    cursor = conn.cursor()

    batch = []
    first_batch = True

    with open(file_path, "r", encoding="utf-8") as f:

        for line in tqdm(f):

            record = json.loads(line)
            clean_record = {}

            for k, v in record.items():
                if isinstance(v, (dict, list)):
                    clean_record[k] = json.dumps(v)   # convert dict/list to string
                else:
                    clean_record[k] = v

            batch.append(clean_record)

            if len(batch) >= BATCH_SIZE:

                df = pd.DataFrame(batch)

                if first_batch:
                    create_table_from_df(cursor, table_name, df)
                    first_batch = False

                insert_batch(cursor, table_name, df)
                conn.commit()

                batch = []

        if batch:

            df = pd.DataFrame(batch)

            if first_batch:
                create_table_from_df(cursor, table_name, df)

            insert_batch(cursor, table_name, df)
            conn.commit()

    cursor.close()
    conn.close()

    print(f"{table_name} completed")


# ==========================
# MAIN
# ==========================

def main():

    for file in os.listdir(JSON_FOLDER):

        if file.endswith(".json"):

            file_path = os.path.join(JSON_FOLDER, file)
            table_name = os.path.splitext(file)[0]

            process_json(file_path, table_name)


if __name__ == "__main__":
    main()