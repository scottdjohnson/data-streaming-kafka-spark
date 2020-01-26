##Project questions
**Question:** How did changing values on the SparkSession property parameters affect the throughput and latency of the data?


What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

**Answer:**

These are the parameters that I tested changing: 

rowsPerSecond: 10, 100, 1000, 10000

maxOffsetsPerTrigger: 100, 200, 1000

I also tested the writeStream trigger function using a processingTime of 5, 10, 20 and 30 seconds

After running with each change for about a minute, I monitored the inputRowsPerSecond and processedRowsPerSecond. I found that, in general, the inputRowsPerSecond was less than the processedRowsPerSecond. However, this was NOT the case when I increased the trigger processingTime from 5 seconds upward 10, 20 and 30 seconds. In this case, the inputRowsPerSecond steadily declined. It is not really clear how Spark can process data more quickly than it can input it. 

Altering rowsPerSecond had no clear result. For example, using a value of 10, 100, 1000, or 10000 rowsPerSecond (while keeping other values constant) showed overall the same inputRowsPerSecond and processedRowsPerSecond.

Most interestingly, I found that altering the value of maxOffsetsPerTrigger had the greatest impact on latency and throughput. Changing maxOffsetsPerTrigger from 100 to 200 increased the average processedRowsPerSecond from about 20 to about 35. Increasing maxOffsetsPerTrigger again to 1000 increased the processedRowsPerSecond to about 180.

With this in mind, this final change had the largest impact on latency.

## Startup instructions
Download Spark and set SPARK_HOME in bash_profile (for Mac and Linux)
In $SPARK_HOME/jars, add spark-sql-kafka-0-10_2.11-2.3.0.jar and kafka-clients-0.10.1.0.jar, assuming a Kafka version 0.10.+ and Spark 2.3.0.

* Launch Kafka Producer

python3 ./kafka_server.py

* Launch Spark Streaming job

spark-submit data_stream.py
