import os
os.environ["PYSPARK_PYTHON"] = "python"

import json
# import os 
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, when, count
from pyspark.sql.types import StringType

# For encryption: import Fernet from cryptography.fernet
from cryptography.fernet import Fernet

# Initialize Spark Session
spark = SparkSession.builder.appName('Encrpted_Data').getOrCreate()

jdbc_url = "jdbc:postgresql://localhost:5432/claimmgmt"
connectionProperties = {
    "user": "shreyash",
    "password": "shreyash@10",
    "driver": "org.postgresql.Driver"
}

tables = ["claim"]
config_file = "encrypt_data.json"
output_dir = "encrypt_data"

if os.path.exists(config_file):
    with open(config_file, 'r') as f:
        extraction_config = json.load(f)
else:
    extraction_config = {}

default_time = "1970-01-01 00:00:00"

# Set up encryption key and cipher
# (In production, do NOT generate a new key each time; load it securely.)
key = Fernet.generate_key()
cipher_suite = Fernet(key)

# Define UDFs for masking and encryption
def mask_pii(value):
    if value is None:
        return None
    return value[0] + "*" * (len(value) - 1)

def encrypt_value(value):
    if value is None:
        return None
    # Encrypt the value and decode it back to string
    encrypted_value = cipher_suite.encrypt(value.encode('utf-8')).decode('utf-8')
    return encrypted_value

mask_udf = spark.udf.register("mask_udf", mask_pii, StringType())
encrypt_udf = spark.udf.register("encrypt_udf", encrypt_value, StringType())

for table in tables:
    last_update_time = extraction_config.get(table, default_time)
    print(f"\nProcessing table '{table}' with last updated timestamp: {last_update_time}")

    query = f"(SELECT * FROM {table} WHERE dateinserted > '{last_update_time}') as sub"

    try:
        dataFrame = spark.read.jdbc(url=jdbc_url, table=query, properties=connectionProperties)
    except Exception as e:
        print(f"Error reading table {table} : {e}")
        continue

    if dataFrame.count() == 0:
        print(f"No new data for table `{table}`")
        continue

    # Aggregate the claim data by ply_name
    dataFrame = dataFrame.groupBy("ply_name").agg(
        count(when(col("claim_status") == "pending", True)).alias("open_claims"),
        count(when(col("claim_status") == "closed", True)).alias("closed_claims")
    )

    # Apply masking and encryption to ply_name
    dataFrame = dataFrame.withColumn("ply_name_masked", mask_udf(col("ply_name"))) \
        .withColumn("ply_name_encrypted", encrypt_udf(col("ply_name")))

    # Write the resulting data to the output directory directly
    parquet_path = output_dir
    dataFrame.write.mode("append").parquet(parquet_path)
    print(f"Data written to Parquet format at: {parquet_path}")

    # For updating extraction timestamp, we use the current timestamp (or compute max(dateinserted) if available)
    new_max_ts_row = dataFrame.select(current_timestamp().alias("max_ts")).collect()
    new_max_ts = new_max_ts_row[0]["max_ts"] if new_max_ts_row else None

    if new_max_ts:
        extraction_config[table] = new_max_ts
        print(f"Updated extraction timestamp for '{table}' to: {new_max_ts}")

with open(config_file, "w") as f:
    json.dump(extraction_config, f, default=str)

spark.stop()
