# Run through pyspark shell using:
# exec(open("test.py").read())
import subprocess as sp
import pandas as pd
from pyspark.sql.functions import udf
from pyspark.sql.functions import *
from pyspark.sql.types import *
if __name__ == '__main__':
    sp.call("cls", shell = True)






    print("----------------------------------------------------------------------")
    print("  UDF Demo")
    print("----------------------------------------------------------------------")
    # Load some data
    filepathParquet = ".\\resources\\parquet\\AdventureWorks-oltp\\Sales.SalesOrderDetail.parquet"
    filepathText = ".\\resources\\txt\\a-scandal-in-bohemia-words-only.txt"
    rdd = spark.sparkContext.textFile(filepathText).cache()
    dfp = spark.read.format("parquet").load(filepathParquet).cache()

    # Declare/register some user-defined functions
    @udf(returnType = DoubleType())
    def standardScale(x, mu, sig): return (x - mu)/sig
    def minMaxScale(x, rng): return x / rng
    spark.udf.register("minMaxScale", minMaxScale, DoubleType())

    mu = dfp.select("LineTotal").agg(mean("LineTotal"))
    #dfp.select(standardScale(""))