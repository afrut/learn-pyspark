from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import Row

def somefunc(row: Row):
    print(row["CarrierTrackingNumber"])

if __name__ == '__main__':
    spark = SparkSession.builder.appName("DataFrameCreation").getOrCreate()

    print("----------------------------------------------------------------------")
    print("  DataFrame Demo")
    print("----------------------------------------------------------------------")
    filepathParquet = ".\\resources\\parquet\\AdventureWorks-oltp\\Sales.SalesOrderDetail.parquet"
    df = spark.read.format("parquet").load(filepathParquet)
    df.cache()

    df.collect() # Return the DataFrame as a list of Row
    df.describe() # Compute basic statistics
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
    df.limit(5).foreach(somefunc) # 5 rows from DataFrame and apply a function to each row
    df.printSchema() # Print schema
    df.show(5) # Show top 5 rows. Default of 20
    df.summary() # Same as describe but with percentiles
    df.tail(5) # Last 5 rows
    df.createOrReplaceTempView("table") # Create a temporary view for using SQL on
    spark.sql("SELECT * FROM table").show()

    
    spark.stop()