#ss StreamingBasics.py
# A script that uses a series of csv files as a streaming source and the console as a streaming sink
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col
from StreamingFunc import timeoutNewData
from confluent_kafka.admin import AdminClient, NewTopic
from time import sleep
from StreamingBasics import _rmdir

# Re-create a kafka topic
def _createTopic(topic: str, ac: AdminClient):
    topics = set(ac.list_topics().topics.keys())
    if topic in topics:
        dctFuture = ac.delete_topics([topic], operation_timeout = 60)
        dctFuture[topic].result()
    while True:
        try:
            dctFuture = ac.create_topics([NewTopic(topic, 1)], operation_timeout = 60)
            dctFuture[topic].result()
            topics = set(ac.list_topics().topics.keys())
            if topic in topics:
                return True
            else:
                return False
        except Exception as e:
            if e.args[0].name() == "TOPIC_ALREADY_EXISTS":
                print("Waiting for topic deletion to complete")
                sleep(1)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Test").getOrCreate()

    # ----------------------------------------
    # Initialization
    # ----------------------------------------
    socketAddress = "[::1]:9092"
    ac = AdminClient({"bootstrap.servers": socketAddress})
    checkpointPath = ".\\resources\\kafka\\job-listing\\checkpoint\\"
    _rmdir(checkpointPath)

    # Delete and re-create kafka topics
    topic = "job-listing-test"
    assert _createTopic(topic, ac), f"{topic} not created!"
    print("Kafka topics re-created")

    # ----------------------------------------
    # csv source to kafka sink
    # ----------------------------------------
    print("Streaming from csv source to kafka sink")
    srcPath = ".\\resources\\csv\\job-listing\\"
    maxFilesPerTrigger = 30
    df = spark.readStream.format("csv")\
        .option("path", srcPath)\
        .option("maxFilesPerTrigger", maxFilesPerTrigger)\
        .load()

    # A column named value is required
    transform = df.withColumn("value", col("_c3")).select("value")

    query = transform.writeStream.format("kafka")\
        .option("kafka.bootstrap.servers", socketAddress)\
        .option("topic", topic)\
        .option("checkpointLocation", checkpointPath)\
        .start()

    timeout = 3
    timeoutNewData(query, timeout)
    print(f"No new data has arrived in {timeout}s. Terminating query")
    query.stop()
    query.awaitTermination()

    # ----------------------------------------
    # kafka source to console sink
    # ----------------------------------------
    print("Streaming from kafka source to console sink")
    checkpointPath = ".\\resources\\kafka\\job-listing\\checkpoint\\"
    maxOffsetsPerTrigger = 30 * 15
    df = spark.readStream.format("kafka")\
        .option("kafka.bootstrap.servers", socketAddress)\
        .option("subscribe", topic)\
        .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)\
        .option("startingOffsets", "earliest")\
        .load()

    # Only get the value column
    transform = df.select(count(col("value")))

    query = transform.writeStream.format("console")\
        .outputMode("complete")\
        .start()

    timeout = 3
    timeoutNewData(query, timeout)
    print(f"No new data has arrived in {timeout}s. Terminating query")
    query.stop()
    query.awaitTermination()

    # ----------------------------------------
    # Cleanup
    # ----------------------------------------
    # Remove checkpoint directory and topic
    _rmdir(checkpointPath)
    ac.delete_topics([topic], operation_timeout = 60)[topic].result()

    spark.stop()
    print("Done")