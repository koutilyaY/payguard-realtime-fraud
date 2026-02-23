import streamlit as st
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("monitoring-dashboard")
    .config("spark.jars.packages","io.delta:delta-spark_2.12:3.2.0")
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

st.title("🚨 PayGuard Production Monitoring")

# Stream Metrics
stream_df = spark.read.format("delta").load("delta/monitoring/stream_metrics_v1")
fraud_df = spark.read.format("delta").load("delta/monitoring/fraud_metrics_v1")
dlq_df = spark.read.format("delta").load("delta/monitoring/dlq_metrics_v1")

st.subheader("Streaming Volume")
st.line_chart(stream_df.toPandas().set_index("batch_id")["records_in_batch"])

st.subheader("Fraud Rate")
fraud_pd = fraud_df.toPandas()
fraud_pd["fraud_rate"] = fraud_pd["fraud_count"] / fraud_pd["total_records"]
st.line_chart(fraud_pd.set_index("batch_id")["fraud_rate"])

st.subheader("DLQ Rate")
st.line_chart(dlq_df.toPandas().set_index("batch_id")["dlq_records"])
