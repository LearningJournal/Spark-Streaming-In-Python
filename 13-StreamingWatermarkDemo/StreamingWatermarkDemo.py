from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_timestamp, col, expr
from pyspark.sql.types import StructType, StructField, StringType

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("Streaming Watermark Demo") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", 2) \
        .getOrCreate()

    logger = Log4j(spark)

    impressionSchema = StructType([
        StructField("ImpressionID", StringType()),
        StructField("CreatedTime", StringType()),
        StructField("Campaigner", StringType())
    ])

    clickSchema = StructType([
        StructField("ImpressionID", StringType()),
        StructField("CreatedTime", StringType())
    ])

    kafka_impression_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "impressions") \
        .option("startingOffsets", "earliest") \
        .load()

    impressions_df = kafka_impression_df \
        .select(from_json(col("value").cast("string"), impressionSchema).alias("value")) \
        .selectExpr("value.ImpressionID", "value.CreatedTime", "value.Campaigner") \
        .withColumn("ImpressionTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss")) \
        .drop("CreatedTime") \
        .withWatermark("ImpressionTime", "30 minute")

    kafka_click_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "clicks") \
        .option("startingOffsets", "earliest") \
        .load()

    clicks_df = kafka_click_df.select(
        from_json(col("value").cast("string"), clickSchema).alias("value")) \
        .selectExpr("value.ImpressionID as ClickID", "value.CreatedTime") \
        .withColumn("ClickTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss")) \
        .drop("CreatedTime") \
        .withWatermark("ClickTime", "30 minute")

    join_expr = "ImpressionID == ClickID"
    join_type = "inner"

    joined_df = impressions_df.join(clicks_df, expr(join_expr), join_type)

    output_query = joined_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("checkpointLocation", "chk-point-dir") \
        .trigger(processingTime="1 minute") \
        .start()

    logger.info("Waiting for Query")
    output_query.awaitTermination()
