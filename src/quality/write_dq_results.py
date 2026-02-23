from datetime import datetime, timezone
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
)

DQ_SCHEMA = StructType([
    StructField("run_ts", TimestampType(), True),
    StructField("suite_name", StringType(), True),
    StructField("layer", StringType(), True),
    StructField("success", IntegerType(), True),
    StructField("success_percent", DoubleType(), True),
    StructField("evaluated_expectations", IntegerType(), True),
    StructField("successful_expectations", IntegerType(), True),
    StructField("unsuccessful_expectations", IntegerType(), True),
    StructField("validation_id", StringType(), True),
])

def write_dq(spark, dq_path: str, suite_name: str, layer: str, results: dict) -> None:
    stats = results["statistics"]
    row = {
        "run_ts": datetime.now(timezone.utc),
        "suite_name": suite_name,
        "layer": layer,
        "success": int(bool(results["success"])),
        "success_percent": float(stats["success_percent"]),
        "evaluated_expectations": int(stats["evaluated_expectations"]),
        "successful_expectations": int(stats["successful_expectations"]),
        "unsuccessful_expectations": int(stats["unsuccessful_expectations"]),
        "validation_id": results.get("meta", {}).get("validation_id", ""),
    }
    df = spark.createDataFrame([row], schema=DQ_SCHEMA)
    df.write.format("delta").mode("append").save(dq_path)
