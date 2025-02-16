from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("ReadParquetFile") \
    .getOrCreate()

# Read the Parquet file (adjust the path if needed)
parquet_path = "output_data/*/parquet"
df = spark.read.parquet(parquet_path)

# Show the DataFrame's contents
df.show()

# Optionally, print the schema
df.printSchema()

# Stop the Spark session when done
spark.stop()
