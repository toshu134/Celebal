import os
import pandas as pd
from sqlalchemy import create_engine
import re
from datetime import datetime

data_lake_path = "./data_lake/"
db_conn_str = "postgresql://username:password@host:port/dbname"
engine = create_engine(db_conn_str)

def extract_date(filename, pattern):
    match = re.search(pattern, filename)
    if match:
        date_str = match.group(1)
        date_formatted = datetime.strptime(date_str, "%Y%m%d").date()
        return date_formatted, date_str
    return None, None

def process_files():
    files = os.listdir(data_lake_path)

    for file in files:
        file_path = os.path.join(data_lake_path, file)

        if file.startswith("CUST_MSTR_") and file.endswith(".csv"):
            date_obj, _ = extract_date(file, r"CUST_MSTR_(\d{8})\.csv")
            if date_obj:
                df = pd.read_csv(file_path)
                df["Date"] = date_obj.strftime("%Y-%m-%d")
                with engine.begin() as conn:
                    conn.execute("TRUNCATE TABLE CUST_MSTR")
                    df.to_sql("CUST_MSTR", con=conn, if_exists="append", index=False)

        elif file.startswith("master_child_export-") and file.endswith(".csv"):
            date_obj, datekey = extract_date(file, r"master_child_export-(\d{8})\.csv")
            if date_obj:
                df = pd.read_csv(file_path)
                df["Date"] = date_obj.strftime("%Y-%m-%d")
                df["DateKey"] = datekey
                with engine.begin() as conn:
                    conn.execute("TRUNCATE TABLE master_child")
                    df.to_sql("master_child", con=conn, if_exists="append", index=False)

        elif file.startswith("H_ECOM_ORDER") and file.endswith(".csv"):
            df = pd.read_csv(file_path)
            with engine.begin() as conn:
                conn.execute("TRUNCATE TABLE H_ECOM_Orders")
                df.to_sql("H_ECOM_Orders", con=conn, if_exists="append", index=False)

if __name__ == "__main__":
    process_files()
