from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window, max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("Sliding Window Demo") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", 1) \
        .getOrCreate()

    logger = Log4j(spark)

    schema = StructType([
        StructField("CreatedTime", StringType()),
        StructField("Reading", DoubleType())
    ])

    kafka_source_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "sensor") \
        .option("startingOffsets", "earliest") \
        .load()

    value_df = kafka_source_df.select(col("key").cast("string").alias("SensorID"),
                                      from_json(col("value").cast("string"), schema).alias("value"))

    sensor_df = value_df.select("SensorID", "value.*") \
        .withColumn("CreatedTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss"))

    agg_df = sensor_df \
        .withWatermark("CreatedTime", "30 minute") \
        .groupBy(col("SensorID"),
                 window(col("CreatedTime"), "15 minute", "5 minute")) \
        .agg(max("Reading").alias("MaxReading"))

    output_df = agg_df.select("SensorID", "window.start", "window.end", "MaxReading")

    window_query = output_df.writeStream \
        .format("console") \
        .outputMode("update") \
        .option("checkpointLocation", "chk-point-dir") \
        .trigger(processingTime="1 minute") \
        .start()

    logger.info("Waiting for Query")
    window_query.awaitTermination()
