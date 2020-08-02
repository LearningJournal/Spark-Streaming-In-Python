from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import expr, col, struct, to_json, sum

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Multi Query Demo") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    logger = Log4j(spark)

    kafka_source_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "invoice-items") \
        .option("startingOffsets", "earliest") \
        .load()

    avroSchema = open('schema/invoice-items', mode='r').read()

    value_df = kafka_source_df.select(from_avro(col("value"), avroSchema).alias("value"))

    rewards_df = value_df.filter("value.CustomerType == 'PRIME'") \
        .groupBy("value.CustomerCardNo") \
        .agg(sum("value.TotalValue").alias("TotalPurchase"),
             sum(expr("value.TotalValue * 0.2").cast("integer")).alias("AggregatedRewards"))

    kafka_target_df = rewards_df.select(expr("CustomerCardNo as key"),
                                        to_json(struct("TotalPurchase", "AggregatedRewards")).alias("value"))

    # kafka_target_df.show(truncate=False)

    rewards_writer_query = kafka_target_df \
        .writeStream \
        .queryName("Rewards Writer") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "customer-rewards") \
        .outputMode("update") \
        .option("checkpointLocation", "chk-point-dir") \
        .start()

    rewards_writer_query.awaitTermination()
