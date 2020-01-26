import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


BOOTSTRAP_SERVER = "localhost:18101"
TOPIC_NAME = "com.udacity.streaming.sfpd"

# TODO Create a schema for incoming resources
schema = StructType([
])

def run_spark_job(spark):

    schema = StructType() \
        .add("crime_id", "string") \
        .add("original_crime_type_name", "string") \
        .add("report_date", "timestamp") \
        .add("call_date", "timestamp") \
        .add("offense_date", "timestamp") \
        .add("call_time", "string") \
        .add("call_date_time", "timestamp") \
        .add("disposition", "string") \
        .add("address", "string") \
        .add("city", "string") \
        .add("state", "string") \
        .add("agency_id", "string") \
        .add("address_type", "string") \
        .add("common_location", "string")

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "earliest") \
        .option("rowsPerSecond", 100) \
        .option("maxOffsetsPerTrigger", 200) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()


    # Show schema for the incoming resources for checks
    df.printSchema()

    kafka_df = df.selectExpr("CAST(value as string)") #\

    kafka_df.printSchema()
    """
    kafka_df.writeStream \
    .format("console") \
    .trigger(processingTime="5 seconds") \
    .option("checkpointLocation", "./checkpoint") \
    .start() \
    .awaitTermination()
    """

    # TODO extract the correct column from the kafka input resources
    service_table = kafka_df.select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")


    # TODO select original_crime_type_name and disposition
    distinct_table = service_table.select("original_crime_type_name", "disposition", "call_date_time")

    # count the number of original crime type
    agg_df = distinct_table.groupBy("original_crime_type_name", "disposition",
                                    psf.window(distinct_table.call_date_time, "1 hour")).count()

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    #query = agg_df.writeStream \
    #    .format("console") \
    #    .trigger(processingTime="5 seconds") \
    #    .outputMode("update") \
    #    .option("checkpointLocation", "./checkpoint") \
    #    .start()

    # TODO attach a ProgressReporter
    #query.awaitTermination()

    # TODO get the right radio code json path
    radio_code_json_filepath = "/Users/scott.johnson/code/sandbox/data-streaming-kafka-spark/spark/radio_code.json"
    radio_code_df = spark.read.option("multiline",True).json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # TODO join on disposition column
    join_query = agg_df.join(radio_code_df, agg_df.disposition == radio_code_df.disposition)

    join_query.writeStream \
        .format("console") \
        .trigger(processingTime="5 seconds") \
        .outputMode("update") \
        .option("checkpointLocation", "./checkpoint") \
        .start() \
        .awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .config('spark.driver.bindAddress','localhost') \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
