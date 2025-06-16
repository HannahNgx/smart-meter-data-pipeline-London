from pyspark.sql import SparkSession
import os

spark = SparkSession.builder \
    .appName("SmartMeterAggregation") \
    .getOrCreate()

INPUT_PATH = os.path.join(os.path.dirname(__file__), '../data/merged_data/merged_data.csv')
df = spark.read.csv(INPUT_PATH, header=True, inferSchema=True)
df.printSchema()
agg_df = df.groupBy("customer_id").sum("daily_energy_usage_kwh")
agg_df = agg_df.withColumnRenamed("sum(daily_energy_usage_kwh)", "total_energy_usage_kwh")
agg_df = agg_df.orderBy("total_energy_usage_kwh", ascending=False)
agg_df.show(10)
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '../data/spark_output/')
agg_df.coalesce(1).write.csv(OUTPUT_DIR, header=True, mode='overwrite')
print(f"Aggregation complete. Output saved to {OUTPUT_DIR}")

spark.stop()
