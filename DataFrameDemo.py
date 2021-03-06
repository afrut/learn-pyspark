#ss DataFrameDemo.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import mean
from pyspark.sql.types import Row
from pyspark.sql.functions import pandas_udf
import pandas as pd
from pyspark.sql.functions import expr
from pyspark.sql.functions import sum
from pyspark.sql.functions import first

if __name__ == '__main__':
    spark = SparkSession.builder.appName("DataFrameCreation").getOrCreate()



    def somefunc(row: Row): print(f"    {row['CarrierTrackingNumber']}")

    print("----------------------------------------------------------------------")
    print("  DataFrame Demo")
    print("----------------------------------------------------------------------")
    filepathParquet = ".\\resources\\parquet\\AdventureWorks-oltp\\Sales.SalesOrderDetail.parquet"
    df = spark.read.format("parquet").load(filepathParquet)
    df.cache()

    df.collect() # Return the DataFrame as a list of Row
    print('Basic statistics')
    df.describe(["UnitPrice"]).show() # Compute basic statistics
    df.dtypes # List of tuples of the types of each column
    df.select("CarrierTrackingNumber") # Select a single column
    df.select(col("CarrierTrackingNumber"))
    df.select(df["CarrierTrackingNumber"])
    df.select("SalesOrderID", "ProductID") # Select multiple columns
    df.select(df["UnitPrice"] / 1000).alias("PriceK") # Select, transform and alias a column
    df.select("ProductID").distinct() # Select unique values of ProductID
    df.filter(df["UnitPrice"] > 1000) # Select only certain rows that fit a criteria
    df.first() # First row of the DataFrame; results in Row
    df.head() # First row of the DataFrame; results in Row
    df.head(5) # First 5 rows of the DataFrame
    print("\n5 carrier tracking numbers")
    df.limit(5).foreach(somefunc) # 5 rows from DataFrame and apply a function to each row
    print('\nSchema:')
    df.printSchema() # Print schema
    df.select("SalesOrderID", "ProductID").show(5) # Show top 5 rows. Default of 20
    df.summary() # Same as describe but with percentiles
    df.tail(5) # Last 5 rows
    df.createOrReplaceTempView("table") # Create a temporary view for using SQL on
    spark.sql("SELECT * FROM table")
    grouped = df.groupBy("SalesOrderID") # Group by a column
    grouped.agg(mean(df["LineTotal"]).alias("AvgLineTotal")) # Aggregate after grouping
    dfpd = df.toPandas() # Convert to pandas DataFrame
    srs = dfpd.loc[:, 'LineTotal'] # Get the series representing a column



    # Create a new DataFrame
    rowData = [("foo","A",10,"14ZX")
        ,("foo","B",14,"52OS")
        ,("foo","C",28,"37AT")
        ,("foo","D",57,"49WE")
        ,("foo","B",13,"14ZX")
        ,("bar","Z",50,"88EE")
        ,("bar","G",24,"33TY")
        ,("bar","O",63,"43BR")
        ,("baz","A",77,"29BT")
        ,("qaz","J",44,"12AB")]
    colnames = ["Name","Letter","Num","Code"]
    df2 = spark.createDataFrame([Row(**dict(zip(colnames, row))) for row in rowData])
    print('Before unpivot:')
    df2.show()

    # Unpivot the DataFrame
    # expr("stack(number of columns, Code1, column1, Code2, column 2 as (ColumnName for Codes, Values)")
    df3 = df2.select("Name", "Letter", expr("stack(2, 'Code', Code, 'Num', string(Num)) as (Column, Value)"))
    print('After unpivot:')
    df3.show()

    # Pivot the DataFrame
    df2.groupBy("Name").pivot("Letter")\
        .agg(sum("Num").alias("Num")
            ,first("Letter").alias("Letter")).show()



    spark.stop()