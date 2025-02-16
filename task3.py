from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date
from pyspark.sql.types import IntegerType, DoubleType, TimestampType

spark = SparkSession.builder.appName("qualityChecks").getOrCreate()

dataFrame = spark.read.parquet("./output_data/policy/parquet")
dataFrame.printSchema()

def run_quality_checks(dataFrame):
    results = {}

    imp_col = ['ply_name','ply_dsc','ply_id','dateinserted']
    
    #Important col check
    for col_name in imp_col:
        missing_count = dataFrame.filter(col(col_name).isNull()).count()
        results[f"Missing {col_name}"] = missing_count
    
    #Unique ply_name check
    ply_name_cnt = dataFrame.groupBy("ply_name").count().filter('count > 1').count()
    results['Duplicate ply_name'] = ply_name_cnt

    #range check
    ply_dsc_exist = dataFrame.filter(col("ply_dsc").isNotNull()).count()
    results["amount check"] = ply_dsc_exist

    #date check
    date = dataFrame.filter(col('dateinserted') > current_date()).count
    results['future date'] = date

    #Data type check
    expected_schema = {
        "ply_id":"integer",
        "claim_id":"integer",
        "ply_hld_id":"integer",
        "ply_amt":"integer",
        "dateinserted":"timestamp"
    }

    for field_name, expected_type in expected_schema.items():
        # Get the actual data type in lower-case string form
        actual_type = [field.dataType.simpleString() for field in dataFrame.schema.fields if field.name == field_name]
        if actual_type:
            actual_type = actual_type[0]
            if actual_type != expected_type:
                results[f"DataType_mismatch_{field_name}"] = f"Expected {expected_type}, got {actual_type}"
        else:
            results[f"Missing_field_{field_name}"] = "Field not found in DataFrame"
            
    return results

# Run the data quality checks
dq_results = run_quality_checks(dataFrame)

# Report the results (you might want to log this or raise alerts in production)
for check, result in dq_results.items():
    print(f"{check}: {result}")

# Stop the Spark session when done
spark.stop()