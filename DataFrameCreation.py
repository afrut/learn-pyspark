from random import randint
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Row
from datetime import date, datetime
import pandas as pd

def randTime(today: date) -> datetime:
    return datetime(today.year
         ,today.month - randint(0, 1)
         ,today.day - randint(0, 10)
         ,randint(0, 23)
         ,randint(0, 59)
         ,randint(0, 59))

if __name__ == '__main__':
    spark = SparkSession.builder.appName("DataFrameCreation").getOrCreate()
    print("----------------------------------------------------------------------")
    print("  DataFrame Creation")
    print("----------------------------------------------------------------------")
    filepathJson = ".\\resources\\json\\iris.json"
    filepathParquet = ".\\resources\\parquet\\AdventureWorks-oltp\\Sales.SalesOrderDetail.parquet"

    # Create DataFrame from json and parquet files
    dfJson = spark.read.format("json").load(filepathJson)
    dfPq = spark.read.format("parquet").load(filepathParquet)

    # Specify a schema
    schema = StructType([
        StructField("sepal_length", DoubleType(), False)
        ,StructField("sepal_width", DoubleType(), False)
        ,StructField("petal_length", DoubleType(), False)
        ,StructField("petal_width", DoubleType(), False)
        ,StructField("target", StringType(), False)
    ])
    df = spark.read.format("json").load(filepathJson, schema = schema)

    # Create from row objects
    today = date.today()
    df2 = spark.createDataFrame([
        Row(ID = 65394, FirstName = "John", LastName = "Smith", DateOfBirth = date(1979, 9, 29), LastActive = randTime(today))
        ,Row(ID = 76405, FirstName = "Jane", LastName = "Doe", DateOfBirth = date(1983, 6, 6), LastActive = randTime(today))
        ,Row(ID = 54283, FirstName = "Jim", LastName = "Raynor", DateOfBirth = date(1992, 2, 22), LastActive = randTime(today))
        ,Row(ID = 32387, FirstName = "Sarah", LastName = "Kerrigan", DateOfBirth = date(1969, 3, 13), LastActive = randTime(today))
        ,Row(ID = 32387, FirstName = "Cloud", LastName = "Strife", DateOfBirth = date(2001, 1, 30), LastActive = randTime(today))
    ])

    # Create DataFrame from pandas DataFrame
    dfpd = pd.read_json(filepathJson)
    df3 = spark.createDataFrame(dfpd)

    # Create DataFrame from RDD,
    rdd = spark.sparkContext.parallelize([
        (65394,76405,54283,32387,32387)
        ,("John","Jane","Jim","Sarah","Cloud")
        ,("Smith","Doe","Raynor","Kerrigan","Strife")
        ,(date(1979, 9, 29),date(1983, 6, 6),date(1992, 2, 22),date(1969, 3, 13),date(2001, 1, 30))
        ,(randTime(today),randTime(today),randTime(today),randTime(today),randTime(today))
    ])
    df4 = spark.createDataFrame(rdd, schema = ["ID", "FirstName", "LastName", "DateOfBirth", "LastActive"])

    # Create an empty DataFrame.
    dfEmpty = spark.createDataFrame([], schema = StructType([]))

    print(f"dfJson number of rows: {dfJson.count()}")
    print(f"dfJson data types:")
    for colname, coltype in dfJson.dtypes:
        print(f"    {colname}: {coltype}")
    print(f"dfPq number of rows: {dfPq.count()}")
    print(f"df number of rows: {df.count()}")
    print(f"df2 number of rows: {df2.count()}")
    print(f"df3 number of rows: {df3.count()}")
    print(f"df4 number of rows: {df4.count()}")
    print(f"dfEmpty number of rows: {dfEmpty.count()}")
    
    spark.stop()