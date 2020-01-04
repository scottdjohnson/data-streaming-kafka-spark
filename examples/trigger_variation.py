import logging
from pyspark.sql import SparkSession


def run_spark_job(spark):
    # TODO set up entry point
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "com.udacity.spark") \
        .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 10) \
        .option("stopGracefullyOnShutdown", "true") \
    .load()
    
    print("*****")
    df.printSchema()
    
    df.selectExpr("CAST(value as string)") \
        .writeStream \
        .format("console") \
        .trigger(processingTime="10 seconds") \
        .option("checkpointLocation", "/home/workspace/checkpoint") \
        .start() \
        .awaitTermination()

    print("*****")

if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("StructuredStreamingSetup") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
