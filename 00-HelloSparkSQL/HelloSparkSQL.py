from pyspark.sql import SparkSession

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("HelloSparkSQL") \
        .getOrCreate()

    logger = Log4j(spark)

    surveyDF = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/sample.csv")

    surveyDF.createOrReplaceTempView("survey_tbl")
    countDF = spark.sql("select Country, count(1) as count from survey_tbl where Age<40 group by Country")

    countDF.show()
