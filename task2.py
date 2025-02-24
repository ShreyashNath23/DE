import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import max as spark_max

# Initialize Spark Session
spark = SparkSession.builder.appName("IncrementalExtraction").getOrCreate()

# JDBC connection properties for PostgreSQL
jdbc_url = "jdbc:postgresql://localhost:5432/claimmgmt"  
connectionProperties = {
    "user": "shreyash",       
    "password": "shreyash@10",   
    "driver": "org.postgresql.Driver"
}


tables = ["policy", "policyholder", "claim"]
config_file = "last_extraction.json"
output_dir = "output_data"  

if os.path.exists(config_file):
    with open(config_file, "r") as f:
        extraction_config = json.load(f)
else:
    extraction_config = {}

default_time = "1970-01-01 00:00:00"

for table in tables:
    last_extraction_time = extraction_config.get(table, default_time)
    print(f"\nProcessing table '{table}' with last extraction timestamp: {last_extraction_time}")

    query = f"(SELECT * FROM {table} WHERE dateinserted > '{last_extraction_time}') as sub"

    try:
        df = spark.read.jdbc(url=jdbc_url, table=query, properties=connectionProperties)
    except Exception as e:
        print(f"Error reading table {table}: {e}")
        continue

    if df.count() == 0:
        print(f"No new data for table '{table}'. Skipping write operations.")
        continue

    
    parquet_path = os.path.join(output_dir, table, "parquet")
    
    
    df.write.mode("append").parquet(parquet_path)
    print(f"Data written to Parquet format at: {parquet_path}")

    new_max_ts_row = df.select(spark_max("dateinserted").alias("max_ts")).collect()
    new_max_ts = new_max_ts_row[0]["max_ts"] if new_max_ts_row else None

    if new_max_ts:
        extraction_config[table] = new_max_ts
        print(f"Updated extraction timestamp for '{table}' to: {new_max_ts}")

with open(config_file, "w") as f:
    json.dump(extraction_config, f, default=str)

print("\nIncremental extraction completed successfully.")
spark.stop()
