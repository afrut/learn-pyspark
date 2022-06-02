from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery
from datetime import datetime
from time import sleep

# Function to terminate query when no new data has arrived in timeout seconds
def timeoutNewData(query: StreamingQuery, timeout: int):
    newData = datetime.now()
    while True:
        sleep(1)
        dct = query.lastProgress
        if dct is not None:
            if dct["numInputRows"] != 0:
                newData = datetime.now()
            if (datetime.now() - newData).total_seconds() >= timeout:
                break

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Test").getOrCreate()
    df = spark.readStream.format("csv")\
        .option("path", ".\\resources\\csv\\job-listing\\")\
        .load()
    query = df.writeStream.format("csv")\
        .outputMode("append")\
        .option("path", ".\\resources\\csv\\job-listing2\\")\
        .option("checkpointLocation", ".\\resources\\csv\\job-listing2\\checkpoint\\")\
        .start()

    timeout = 3
    timeoutNewData(query, timeout)
    print(f"No new data has arrived in {timeout}s. Stopping streaming query")

    query.stop()
    query.awaitTermination()
    spark.stop()
    print("Done")