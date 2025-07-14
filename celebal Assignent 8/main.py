from pyspark.sql.functions import col, sum as _sum, avg, count, unix_timestamp, lit, max as _max, to_date
from pyspark.sql.window import Window
import urllib.request


dbfs_base_path = "/mnt/data_taxi_analytics_2020jan"
csv_file_path = f"{dbfs_base_path}/raw/yellow_tripdata_2020_01.csv"
parquet_output_path = f"{dbfs_base_path}/parquet_output"


file_url = "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2020-01.csv"
local_dbfs_file_path = f"/dbfs{csv_file_path}"
urllib.request.urlretrieve(file_url, local_dbfs_file_path)


df_taxi = spark.read.csv(csv_file_path, header=True, inferSchema=True)


df_taxi.write.mode("overwrite").parquet(parquet_output_path)


df_parquet = spark.read.parquet(parquet_output_path)
df_parquet.createOrReplaceTempView("yellow_taxi_data")


df_with_revenue = df_parquet.withColumn("Revenue", 
    col("fare_amount") + col("extra") + col("mta_tax") + 
    col("improvement_surcharge") + col("tip_amount") + 
    col("tolls_amount") + col("total_amount")
)

df_with_revenue.select("Revenue", "fare_amount", "total_amount").show(5)


df_passenger_by_area = df_parquet.groupBy("PULocationID") \
    .agg(_sum("passenger_count").alias("total_passengers")) \
    .orderBy("total_passengers", ascending=False)

df_passenger_by_area.show()


df_avg_vendor_earnings = df_parquet.groupBy("VendorID").agg(
    avg("fare_amount").alias("average_fare"),
    avg("total_amount").alias("average_total_earning")
)

df_avg_vendor_earnings.show()


payment_window = Window.partitionBy("payment_type") \
    .orderBy("tpep_pickup_datetime") \
    .rowsBetween(-10, 0)

df_payment_moving = df_parquet.withColumn("moving_payment_count", count("*").over(payment_window))

df_payment_moving.select("payment_type", "tpep_pickup_datetime", "moving_payment_count").show(5)


df_with_date = df_parquet.withColumn("trip_date", to_date("tpep_pickup_datetime"))

df_vendor_gain = df_with_date.groupBy("VendorID", "trip_date").agg(
    _sum("passenger_count").alias("total_passengers"),
    _sum("trip_distance").alias("total_distance")
)

df_top2_vendors_on_date = df_vendor_gain.filter(col("trip_date") == '2020-01-15') \
    .orderBy("total_passengers", ascending=False)

df_top2_vendors_on_date.show(2)


df_route_passenger_count = df_parquet.groupBy("PULocationID", "DOLocationID").agg(
    _sum("passenger_count").alias("total_passengers")
).orderBy("total_passengers", ascending=False)

df_route_passenger_count.show(1)


latest_timestamp = df_parquet.select(_max("tpep_pickup_datetime")).first()[0]
latest_unix_time = unix_timestamp(lit(latest_timestamp))

df_recent_rides = df_parquet.withColumn("pickup_unix", unix_timestamp("tpep_pickup_datetime"))

df_last10_seconds = df_recent_rides.filter(
    col("pickup_unix") >= latest_unix_time - 10
)

df_last10_seconds.groupBy("PULocationID").agg(
    _sum("passenger_count").alias("passengers_in_10s")
).orderBy("passengers_in_10s", ascending=False).show()
