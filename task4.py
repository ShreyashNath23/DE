# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, when, count, udf, lit
# from pyspark.sql.types import StringType

# Initialize Spark Session
# spark = SparkSession.builder.appName("PolicyClaimAggregation").getOrCreate()

# Read the incremental data for policy and claim.
# (Assumes these datasets are updated incrementally.)
# policy_dataFrame = spark.read.parquet("output_data/policy/parquet")
# claim_dataFrame = spark.read.parquet("output_data/claim/parquet")

# print("Polciy: ", policy_dataFrame)
# print("Claim: ", claim_dataFrame) 

# --- Step 1: Aggregate Claims by Policy ---

# 'status' column indicating 'Open' or 'Closed' claims.
# claim_agg = claim_dataFrame.groupBy("ply_name").agg(
#     count(when(col("claim_status") == "pending", True)).alias("open_claims"),
#     count(when(col("claim_status") == "closed", True)).alias("closed_claims")
# )

# print("claim_agg: ", claim_agg)

# ********************************************************************

# --- Step 2: PII Data Masking ---
# Suppose policy_dataFrame has a sensitive column 'policyholder_name' that we want to mask.
# This UDF will mask all but the first character.
# def mask_pii(value):
#     if value is None:
#         return None
#     return value[0] + "*" * (len(value) - 1)

# mask_udf = udf(mask_pii, StringType())

# Create a new column with masked PII data
# policy_dataFrame_masked = policy_dataFrame.withColumn("policy_name_masked", mask_udf(col("ply_name")))

# (Optionally, drop the original PII column)
# policy_dataFrame_masked = policy_dataFrame_masked.drop("ply_name")

# --- Step 3: Join Policy with Aggregated Claim Data ---
# final_dataFrame = policy_dataFrame_masked.join(claim_agg, on="ply_id", how="left")

# Fill null values for policies with no claims
# final_dataFrame = final_dataFrame.fillna({"open_claims": 0, "closed_claims": 0})

# --- Step 4: (Optional) Write out the aggregated dataset ---
# You can write this as a new dataset that can be incrementally updated.
# final_dataFrame.write.mode("overwrite").parquet("output_data/aggregated_policy_claims")

# Show the results
# final_dataFrame.show(truncate=False)
# final_dataFrame.printSchema()

# spark.stop()

# *******************************************************************

import json
import os 
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, when, count

spark = SparkSession.builder\
        .appName('Open_and_Closed_Claims')\
        .getOrCreate()

jdbc_url = "jdbc:postgresql://localhost:5432/claimmgmt"
connectionProperties = {
    "user":"shreyash",
    "password":"shreyash@10",
    "driver":"org.postgresql.Driver"
}

tables = ["claim"]
config_file = "claim_data.json"
output_dir = "claim_count_data"

if os.path.exists(config_file):
    with open(config_file, 'r') as f:
        extraction_config = json.load(f)
else:
    extraction_config = {}

default_time = "1970-01-01 00:00:00"

for table in tables:
    last_update_time = extraction_config.get(table, default_time)
    print(f"\nProcessing table '{table}' with last updated timestamp: {last_update_time}")

    query = f"(SELECT * FROM {table} WHERE dateinserted > '{last_update_time}') as sub"

    try:
        dataFrame = spark.read.jdbc(url=jdbc_url,table=query,properties=connectionProperties)
    except Exception as e:
        print(f"Error reading table {table} : {e}")
        continue

    if dataFrame.count() == 0:
        print(f"No new data for table `{table}`")
        continue

    dataFrame = dataFrame.groupBy("ply_name").agg(
        count(when(col("claim_status") == "pending", True)).alias("open_claims"),
        count(when(col("claim_status") == "closed", True)).alias("closed_claims")
    )

    parquet_path = output_dir


    # print(parquet_path)

    dataFrame.write.mode("append").parquet(parquet_path)
    print(f"Data written to Parquet format at: {parquet_path}")

    new_max_ts_row = dataFrame.select(current_timestamp().alias("max_ts")).collect()
    new_max_ts = new_max_ts_row[0]["max_ts"] if new_max_ts_row else None

    if new_max_ts:
        extraction_config[table] = new_max_ts
        print(f"Updated extraction timestamp for '{table}' to: {new_max_ts}")

with open(config_file, "w") as f:
    json.dump(extraction_config, f, default=str)

spark.stop()