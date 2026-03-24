from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, sum, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Create a Spark session
spark = SparkSession.builder.appName("RideSharingAnalytics").getOrCreate()

# Define the schema for incoming JSON data
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Read streaming data from socket
init_df = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Parse JSON data into columns using the defined schema
result_df = init_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Convert timestamp column to TimestampType and add a watermark
stream_df = result_df \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .withWatermark("timestamp", "10 seconds")

# Compute aggregations: total fare and average distance grouped by driver_id
aggregated_df = stream_df.groupBy("driver_id").agg(
    sum("fare_amount").alias("total_fare"),
    avg("distance_km").alias("avg_distance")
)

# Define a function to write each batch to a CSV file
def write_to_csv_batch(batch_df, batch_id):
    # Save the batch DataFrame as a CSV file with the batch ID in the filename
    output_path = "outputs/task_2/files"
    batch_df.write.format("csv") \
        .option("header", "true") \
        .mode("append") \
        .save(output_path)

# Use foreachBatch to apply the function to each micro-batch
query = aggregated_df.writeStream \
    .foreachBatch(write_to_csv_batch) \
    .outputMode("update") \
    .start()

query.awaitTermination()
