import os
import pandas as pd
import mysql.connector
from mysql.connector import Error


# -------------------------------
# 1. DATABASE CONNECTION
# -------------------------------
def create_connection():
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="123456",
            database="tech_layoffs_hiring_trends"
        )
        if conn.is_connected():
            print("✅ Connected to MySQL")
        return conn
    except Error as e:
        print(f"❌ Error connecting to MySQL: {e}")
        return None


# -------------------------------
# 2. CREATE TABLE FROM CSV
# -------------------------------
def create_table_from_csv(cursor, table_name, df):

    columns = []

    for col, dtype in zip(df.columns, df.dtypes):
        if "int" in str(dtype):
            mysql_type = "INT"
        elif "float" in str(dtype):
            mysql_type = "FLOAT"
        elif "datetime" in str(dtype):
            mysql_type = "DATETIME"
        elif "object" in str(dtype):
            mysql_type = "VARCHAR(255)"
        else:
            mysql_type = "TEXT"

        columns.append(f"`{col}` {mysql_type}")

    columns_sql = ", ".join(columns)

    # overwrite table every run (safe for analytics)
    cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")

    create_table_query = f"""
        CREATE TABLE `{table_name}` (
            {columns_sql}
        );
    """

    cursor.execute(create_table_query)
    print(f"📌 Table `{table_name}` created.")


# -------------------------------
# 3. INSERT CSV DATA INTO MYSQL
# -------------------------------
def insert_data(cursor, conn, table_name, df):

    placeholders = ", ".join(["%s"] * len(df.columns))
    columns = ", ".join([f"`{col}`" for col in df.columns])

    insert_query = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"

    # handle null values
    df = df.fillna("")

    # chunk upload for large files
    for i in range(0, len(df), 1000):
        chunk = df.iloc[i:i+1000]
        data = [tuple(row) for row in chunk.values]

        cursor.executemany(insert_query, data)
        conn.commit()

    print(f"🚀 Data inserted into `{table_name}`")


# -------------------------------
# 4. LOAD CSV FILES FROM FOLDER
# -------------------------------

def load_single_csv_to_mysql(file_path, conn):
    cursor = conn.cursor()

    table_name = os.path.basename(file_path).replace(".csv", "").lower()

    print(f"\n📂 Processing file: {file_path}")

    df = pd.read_csv(file_path)

    create_table_from_csv(cursor, table_name, df)
    insert_data(cursor, conn, table_name, df)

    cursor.close()


# -------------------------------
# 5. MAIN FUNCTION
# -------------------------------
if __name__ == "__main__":

    CSV_FILE = r"D:\Data Analytics\tech_layoffs_hiring_trends\tech_layoffs_hiring_trends.csv"

    connection = create_connection()

    if connection:
        load_single_csv_to_mysql(CSV_FILE, connection)
        connection.close()
        print("\n🎉 CSV file loaded successfully!")
