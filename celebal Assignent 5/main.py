import pandas as pd
from sqlalchemy import create_engine
from pyspark.sql import SparkSession

def extract_convert_and_copy(table_name, source_db_url, target_db_url, output_dir, columns=None):
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("Data Integration Pipeline") \
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.4.1") \
        .getOrCreate()

    # Create SQLAlchemy engines
    source_engine = create_engine(source_db_url)
    target_engine = create_engine(target_db_url)

    # Build SQL query
    if columns:
        column_str = ', '.join(columns)
        query = f"SELECT {column_str} FROM {table_name}"
    else:
        query = f"SELECT * FROM {table_name}"

    # Load data into pandas
    print(f"Extracting data from: {table_name}")
    df = pd.read_sql(query, con=source_engine)

    # Convert to Spark DataFrame
    sdf = spark.createDataFrame(df)

    # Write to CSV
    csv_path = f"{output_dir}/{table_name}/csv"
    print(f"Writing CSV to {csv_path}")
    sdf.write.mode("overwrite").csv(csv_path, header=True)

    # Write to Parquet
    parquet_path = f"{output_dir}/{table_name}/parquet"
    print(f"Writing Parquet to {parquet_path}")
    sdf.write.mode("overwrite").parquet(parquet_path)

    # Write to Avro
    avro_path = f"{output_dir}/{table_name}/avro"
    print(f"Writing Avro to {avro_path}")
    sdf.write.format("avro").mode("overwrite").save(avro_path)

    # Copy to target database
    print(f"Copying {table_name} to target database")
    df.to_sql(table_name, con=target_engine, if_exists='replace', index=False)

    spark.stop()
    print(f"✅ Finished processing table: {table_name}\n")


# === CONFIGURATION ===
source_db_url = "postgresql://username:password@localhost:5432/source_db"
target_db_url = "postgresql://username:password@localhost:5432/target_db"
output_directory = "/tmp/data_exports"  # Change this to your local or cloud path
tables = [
    {"name": "customers", "columns": None},  # Full table
    {"name": "orders", "columns": ["order_id", "customer_id", "amount"]}  # Selective columns
]

# === EXECUTE FOR EACH TABLE ===
for table in tables:
    extract_convert_and_copy(
        table_name=table["name"],
        source_db_url=source_db_url,
        target_db_url=target_db_url,
        output_dir=output_directory,
        columns=table["columns"]
    )
