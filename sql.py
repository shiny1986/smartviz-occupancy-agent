import sqlite3
import csv
import os
from pathlib import Path
import sys

DATABASE_FILE = "Smartviz.db"
CSV_FILE = "metrics_app_timeaggregated_202506111450_backup.csv"
TABLE_NAME = "METRICS"

SCRIPT_DIR = Path(__file__).resolve().parent
DB_PATH = SCRIPT_DIR / DATABASE_FILE
CSV_PATH = SCRIPT_DIR / CSV_FILE

def create_metrics_table(conn,columns):
    """
    Dynamically creates the METRICS table based on the CSV header columns
    if the table does not already exist.
    """
    column_defs = [f"{col} TEXT" for col in columns]
    column_defs_str = ', '.join(column_defs)

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    _id INTEGER PRIMARY KEY AUTOINCREMENT,
    {column_defs_str}
    );
    """
    try:
        conn = None
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
    except sqlite3.Error as e:
        print(f"Error creating table: {e}")

def import_csv_to_sqlite():
    """ Reads data from a CSV file and inserts it into a SQLite table """
    if not CSV_PATH.exists():
        print(f"Error: CSV file not found at '{CSV_PATH}'")
        # Now we know exactly where Python is looking
        print(f"The script is running from: {SCRIPT_DIR}")
        return
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        with open(CSV_PATH,'r',newline='',encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)
            columns = [col.strip().replace('','_') for col in header]

            create_metrics_table(conn,columns)
            f.seek(0)
            cursor = conn.cursor()

            columns_str = ', '.join(columns)
            placeholders = ', '.join(['?' for _ in columns])
            insert_query = f"INSERT INTO {TABLE_NAME} ({columns_str}) VALUES ({placeholders})"

            data_to_insert = []
            for i,row in enumerate(reader):
                if not any(row):
                    continue
                if len(row) == len(columns):
                    data_to_insert.append(row)
                else:
                    print("Mismatch")
            
            cursor.executemany(insert_query,data_to_insert)
            conn.commit()
    except sqlite3.OperationalError as e:
        print("Database error")
        print(f"Details:{e}")
    except Exception as e:
        print("Unexpected error")
        print(f"Details:{e}")
        import traceback
        traceback.print_exc()
    finally:
        # Closing the connection
        if conn:
            conn.close()
if __name__ == "__main__":
    import_csv_to_sqlite()

