from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("ReadAvroFile") \
    .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.3") \
    .getOrCreate()

# Define the Avro file path (adjust the path if needed)
avro_path = "output_data/*/avro"

# Read the Avro files using the Avro format
df = spark.read.format("avro").load(avro_path)

# Show the DataFrame's contents
df.show()

# Optionally, print the schema
df.printSchema()

# Stop the Spark session when done
spark.stop()
