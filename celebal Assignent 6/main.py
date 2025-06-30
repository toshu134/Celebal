import psycopg2
import pandas as pd
import datetime
import time
import os

# --- Configuration ---
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'your_db',
    'user': 'your_username',
    'password': 'your_password'
}
WATERMARK_FILE = 'last_watermark.txt'
OUTPUT_FILE = 'data_output.csv'

MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

TABLE_NAME = 'your_table'
TIMESTAMP_COLUMN = 'last_modified'  # your watermark column name


# --- Helper Functions ---
def is_last_saturday():
    today = datetime.date.today()
    last_day = today.replace(day=28) + datetime.timedelta(days=4)  # next month
    last_day = last_day - datetime.timedelta(days=last_day.day)
    return today.weekday() == 5 and today + datetime.timedelta(days=7) > last_day

def get_last_watermark():
    if not os.path.exists(WATERMARK_FILE):
        return '1970-01-01 00:00:00'
    with open(WATERMARK_FILE, 'r') as f:
        return f.read().strip()

def save_new_watermark(new_watermark):
    with open(WATERMARK_FILE, 'w') as f:
        f.write(new_watermark)

def fetch_data():
    last_watermark = get_last_watermark()

    query = f"""
    SELECT * FROM {TABLE_NAME}
    WHERE {TIMESTAMP_COLUMN} > %s
    ORDER BY {TIMESTAMP_COLUMN} ASC;
    """

    retries = 0
    while retries < MAX_RETRIES:
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            df = pd.read_sql(query, conn, params=(last_watermark,))
            conn.close()
            return df
        except Exception as e:
            print(f"Error connecting to DB: {e}")
            retries += 1
            time.sleep(RETRY_DELAY)
    raise Exception("Max retries reached. Could not connect to the database.")

# --- Main Execution ---
if is_last_saturday():
    print("Running job (Last Saturday)...")
    df = fetch_data()

    if not df.empty:
        df.to_csv(OUTPUT_FILE, index=False)
        new_max = df[TIMESTAMP_COLUMN].max().strftime('%Y-%m-%d %H:%M:%S')
        save_new_watermark(new_max)
        print(f"Data saved. New watermark: {new_max}")
    else:
        print("No new data to process.")
else:
    print("Today is not the last Saturday. Skipping.")
