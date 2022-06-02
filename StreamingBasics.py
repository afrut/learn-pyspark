#python StreamingBasics.py
# A script that uses a series of csv files as a streaming source and the console as a streaming sink
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Test").getOrCreate()
    df = spark.readStream.format("csv")\
        .option("path", ".\\resources\\csv\\job-listing\\")\
        .option("maxFilesPerTrigger", 1)\
        .load()

    query = df.select(count(col(df.columns[0])).alias("count"))\
        .writeStream.format("console")\
        .outputMode("complete")\
        .start()

    query.awaitTermination()
    print("Done")