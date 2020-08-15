from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Tumbling Window Demo") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    logger = Log4j(spark)

    invoice_schema = StructType([
        StructField("InvoiceNumber", StringType()),
        StructField("CreatedTime", StringType()),
        StructField("StoreID", StringType()),
        StructField("TotalAmount", DoubleType())
    ])

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "invoices") \
        .option("startingOffsets", "earliest") \
        .load()

    value_df = kafka_df.select(from_json(col("value").cast("string"), invoice_schema).alias("value"))

    # value_df.printSchema()
    # value_df.show(truncate=False)

    invoice_df = value_df.select("value.*") \
        .withColumn("CreatedTime", to_timestamp("CreatedTime", "yyyy-MM-dd HH:mm:ss"))

    count_df = invoice_df.groupBy("StoreID",
                                  window("CreatedTime", "5 minute")).count()

    # count_df.printSchema()
    # count_df.show(truncate=False)

    output_df = count_df.select("StoreID", "window.start", "window.end", "count")

    windowQuery = output_df.writeStream \
        .format("console") \
        .outputMode("update") \
        .option("checkpointLocation", "chk-point-dir") \
        .trigger(processingTime="1 minute") \
        .start()

    logger.info("Counting Invoices")
    windowQuery.awaitTermination()
