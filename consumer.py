import os
os.environ['HADOOP_HOME'] = 'C:\\hadoop'
os.environ['PATH'] = os.environ['PATH'] + ';C:\\hadoop\\bin'

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as spark_sum, desc
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    IntegerType, DoubleType
)

print("⏳ Spark shuru ho raha hai - 30-60 seconds lagenge...")
print("⏳ Spark shuru ho raha hai - 30-60 seconds lagenge...")

print("⏳ Spark shuru ho raha hai - 30-60 seconds lagenge...")
# Spark Session banao
spark = SparkSession.builder \
    .appName("KafkaOrderProcessor") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0"
    ) \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

# Logs kam karo - sirf errors dikhao
spark.sparkContext.setLogLevel("ERROR")

print("✅ Spark ready hai!\n")

# Order ka structure define karo
# Kafka se data bytes mein aata hai
# Hum batate hain Spark ko - isko is format mein samajhna hai
schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_name", StringType()),
    StructField("customer_city", StringType()),
    StructField("product", StringType()),
    StructField("category", StringType()),
    StructField("price_per_unit", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("total_amount", DoubleType()),
    StructField("status", StringType()),
    StructField("timestamp", StringType())
])

# Kafka se data padhna shuru karo
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "latest") \
    .load()

# Bytes → JSON → Proper columns
orders = raw_stream.select(
    from_json(
        col("value").cast("string"), 
        schema
    ).alias("data")
).select("data.*")

# Real-time aggregation
# Har product ka total revenue calculate karo
revenue_by_product = orders \
    .groupBy("product", "category") \
    .agg(
        spark_sum("total_amount").alias("total_revenue"),
        spark_sum("quantity").alias("units_sold")
    )

# Console pe dikhao - har 5 second mein update
print("📊 Live Revenue Dashboard shuru ho raha hai...")
print("Har 5 second mein update hoga\n")

query = revenue_by_product.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 20) \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()
