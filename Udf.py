from pyspark.sql import SparkSession
import pandas as pd
import math
from pyspark.sql.functions import mean
from pyspark.sql.functions import stddev
from pyspark.sql.functions import min
from pyspark.sql.functions import max
from pyspark.sql.functions import udf
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import *

class Funcs(object):
    @staticmethod
    def standardScale(x): return (x - mu) / sig

    @staticmethod
    def minMaxScale(x): return (x - mu) / sig

    @staticmethod
    def normalize(srs: pd.Series) -> pd.Series:
        return srs / math.sqrt(srs.map(lambda x: x**2).sum())

if __name__ == '__main__':
    spark = SparkSession.builder.appName("DataFrameCreation").getOrCreate()



    print("----------------------------------------------------------------------")
    print("  UDF Demo")
    print("----------------------------------------------------------------------")
    # Load some data
    filepathParquet = ".\\resources\\parquet\\AdventureWorks-oltp\\Sales.SalesOrderDetail.parquet"
    filepathText = ".\\resources\\txt\\a-scandal-in-bohemia-words-only.txt"
    rdd = spark.sparkContext.textFile(filepathText).cache()
    df = spark.read.format("parquet").load(filepathParquet).cache()

    # Compute some basic statistics
    mu = df.select("LineTotal").agg(mean("LineTotal").alias("mean")).collect()[0].mean
    sig = df.select("LineTotal").agg(stddev("LineTotal").alias("stddev")).collect()[0].stddev
    rng = df.select(max("LineTotal").alias("max")).collect()[0].max -\
        df.select(min("LineTotal").alias("min")).collect()[0].min

    # Declare some user-defined functions and use in DataFrame
    standardScale = udf(Funcs.standardScale )
    df.select(standardScale("LineTotal").alias("StandardScaled"))

    # Register a UDF and use with SQL
    spark.udf.register("minMaxScale", Funcs.minMaxScale, DoubleType())
    df.createOrReplaceTempView("table")
    spark.sql("SELECT minMaxScale(LineTotal) MinMaxScaled "
              "FROM table ")

    # Declare and use a pandas_udf
    normalize = pandas_udf(Funcs.normalize, DoubleType()) # Type has to match spark DataFrame type
    df.withColumn("LineTotal", df["LineTotal"].cast(DoubleType())) \
        .select(normalize("LineTotal").alias("LineTotal"))
    # df.select(normalize("Linetotal").alias("Normalized")).show()



    spark.stop()