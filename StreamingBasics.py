#ss StreamingBasics.py
# A script that uses a series of csv files as a streaming source and the console as a streaming sink
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col
from StreamingFunc import timeoutNewData
from shutil import rmtree
from confluent_kafka.admin import AdminClient, NewTopic

# Delete a directory and its contents if it exists
def _rmdir(dirPath: str):
    if os.path.exists(dirPath):
        rmtree(dirPath)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Test").getOrCreate()

    # ----------------------------------------
    # Initialization
    # ----------------------------------------
    # Delete from previous runs
    _rmdir(r"D:\src\learn-pyspark\resources\csv\job-listing2")
    _rmdir(r"D:\src\learn-pyspark\resources\json\job-listing2")
    _rmdir(r"D:\src\learn-pyspark\resources\parquet\job-listing2")
    _rmdir(r"D:\src\learn-pyspark\resources\delta\job-listing2")

    # ----------------------------------------
    # CSV source to console sink
    # ----------------------------------------
    print("Streaming from csv source to console sink")
    srcPath = ".\\resources\\csv\\job-listing\\"
    maxFilesPerTrigger = 30
    df = spark.readStream.format("csv")\
        .option("path", srcPath)\
        .option("maxFilesPerTrigger", maxFilesPerTrigger)\
        .load()
    query = df.select(count(col(df.columns[0])).alias("count"))\
        .writeStream.format("console")\
        .outputMode("complete")\
        .start()

    timeout = 3
    timeoutNewData(query, timeout)
    print(f"No new data has arrived in {timeout}s. Terminating query")
    query.stop()
    query.awaitTermination()

    # ----------------------------------------
    # CSV source to CSV sink
    # ----------------------------------------
    print("Streaming from csv source to csv sink")
    srcPath = ".\\resources\\csv\\job-listing\\"
    sinkPath = ".\\resources\\csv\\job-listing2\\"
    checkpointPath = sinkPath + "checkpoint\\"
    maxFilesPerTrigger = 30
    df = spark.readStream.format("csv")\
        .option("path", srcPath)\
        .option("maxFilesPerTrigger", maxFilesPerTrigger)\
        .load()
    query = df.writeStream.format("csv")\
        .outputMode("append")\
        .option("path", sinkPath)\
        .option("checkpointLocation", checkpointPath)\
        .start()

    timeout = 3
    timeoutNewData(query, timeout)
    print(f"No new data has arrived in {timeout}s. Terminating query")
    query.stop()
    query.awaitTermination()

    # ----------------------------------------
    # JSON source to JSON sink
    # ----------------------------------------
    print("Streaming from json source to json sink")
    srcPath = ".\\resources\\json\\job-listing\\"
    sinkPath = ".\\resources\\json\\job-listing2\\"
    checkpointPath = sinkPath + "checkpoint\\"
    maxFilesPerTrigger = 30
    df = spark.readStream.format("json")\
        .option("path", srcPath)\
        .option("maxFilesPerTrigger", maxFilesPerTrigger)\
        .load()
    query = df.writeStream.format("json")\
        .outputMode("append")\
        .option("path", sinkPath)\
        .option("checkpointLocation", checkpointPath)\
        .start()

    timeout = 3
    timeoutNewData(query, timeout)
    print(f"No new data has arrived in {timeout}s. Terminating query")
    query.stop()
    query.awaitTermination()

    # ----------------------------------------
    # parquet source to parquet sink
    # ----------------------------------------
    print("Streaming from parquet source to parquet sink")
    srcPath = ".\\resources\\parquet\\job-listing\\"
    sinkPath = ".\\resources\\parquet\\job-listing2\\"
    checkpointPath = sinkPath + "checkpoint\\"
    maxFilesPerTrigger = 30
    df = spark.readStream.format("parquet")\
        .option("path", srcPath)\
        .option("maxFilesPerTrigger", maxFilesPerTrigger)\
        .load()
    query = df.writeStream.format("parquet")\
        .outputMode("append")\
        .option("path", sinkPath)\
        .option("checkpointLocation", checkpointPath)\
        .start()

    timeout = 3
    timeoutNewData(query, timeout)
    print(f"No new data has arrived in {timeout}s. Terminating query")
    query.stop()
    query.awaitTermination()

    # ----------------------------------------
    # delta source to delta sink
    # ----------------------------------------
    print("Streaming from delta source to delta sink")
    srcPath = ".\\resources\\delta\\job-listing\\"
    sinkPath = ".\\resources\\delta\\job-listing2\\"
    checkpointPath = sinkPath + "checkpoint\\"
    maxFilesPerTrigger = 30
    df = spark.readStream.format("delta")\
        .option("path", srcPath)\
        .option("maxFilesPerTrigger", maxFilesPerTrigger)\
        .load()
    query = df.writeStream.format("delta")\
        .outputMode("append")\
        .option("path", sinkPath)\
        .option("checkpointLocation", checkpointPath)\
        .start()

    timeout = 3
    timeoutNewData(query, timeout)
    print(f"No new data has arrived in {timeout}s. Terminating query")
    query.stop()
    query.awaitTermination()

    spark.stop()
    print("Done")