from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import dense_rank
from pyspark.sql.window import Window
from pyspark.sql.functions import when
from pyspark.sql.functions import concat
from pyspark.sql.functions import lit
from pyspark.sql.functions import sum

if __name__ == '__main__':
    spark = SparkSession.builder.appName("AdventureWorks").getOrCreate()
    print("----------------------------------------------------------------------")
    print("  AdventureWorks Oltp")
    print("----------------------------------------------------------------------")

    # Rank products by their profit margin
    def productByMargin():
        # Load Data
        product = spark.read.format("parquet")\
            .load(".\\resources\\parquet\\AdventureWorks-oltp\\Production.Product.parquet")
        colNames = ["Name","Margin","Rank"]

        # Compute margin and rank
        wdw = Window.orderBy(col("Margin").desc())
        prodMargins = product \
            .withColumn("Margin", col("ListPrice") - col("StandardCost")) \
            .withColumn("Rank", dense_rank().over(wdw)) \
            .select(colNames)
        print("----------------------------------------------------------------------")
        print("  Product Margins")
        print("----------------------------------------------------------------------")
        prodMargins.show()

    # Find the top 3 products sold by each SalesPerson by quantity sold
    def productBySalesPersonByQuantity():
        pass

        # Load Data
        sod = spark.read.format("parquet").load(".\\resources\\parquet\\AdventureWorks-oltp\\Sales.SalesOrderDetail.parquet")
        soh = spark.read.format("parquet").load(".\\resources\\parquet\\AdventureWorks-oltp\\Sales.SalesOrderHeader.parquet")
        product = spark.read.format("parquet").load(".\\resources\\parquet\\AdventureWorks-oltp\\Production.Product.parquet")
        sp = spark.read.format("parquet").load(".\\resources\\parquet\\AdventureWorks-oltp\\Sales.SalesPerson.parquet")
        person = spark.read.format("parquet").load(".\\resources\\parquet\\AdventureWorks-oltp\\Person.Person.parquet")

        # All sales
        sales = sod\
            .join(soh, sod["SalesOrderID"] == soh["SalesOrderID"], "inner") \
            .join(product, sod["ProductID"] == product["ProductID"], "inner") \
            .join(sp, soh["SalesPersonID"] == sp["BusinessEntityID"], "left") \
            .join(person, soh["SalesPersonID"] == person["BusinessEntityID"], "left") \
            .withColumn("SalesPersonName"
                ,when(col("SalesPersonID").isNull(), "ONLINE")\
                    .otherwise(concat(person["LastName"], lit(", "), person["FirstName"]))) \
            .select("SalesPersonName"
                ,product["ProductID"]
                ,product["Name"].alias("ProductName")
                ,sod["OrderQty"])

        # All sales grouped by SalesPersonName and ProductID and summing OrderQty
        salesGrouped = sales.groupby("SalesPersonName", "ProductID") \
            .agg(sum("OrderQty").alias("OrderQty")) \
            .select("SalesPersonName", "ProductID", "OrderQty")

        # For every sales person, compute their top 3 products
        wdw = Window.partitionBy(col("SalesPersonName")).orderBy(col("OrderQty").desc())
        topProducts = salesGrouped.withColumn("Rank", dense_rank().over(wdw)) \
            .filter(col("Rank") <= 3) \
            .orderBy(col("SalesPersonName").asc(), col("Rank").asc())

        print("----------------------------------------------------------------------")
        print("  Product Margins")
        print("----------------------------------------------------------------------")
        topProducts.show()

    # Find the top 3 products sold by each SalesPerson by revenue
    def productBySalesPersonByRevenue():
        sod = spark.read.format("parquet").load(".\\resources\\parquet\\AdventureWorks-oltp\\Sales.SalesOrderDetail.parquet")
        soh = spark.read.format("parquet").load(".\\resources\\parquet\\AdventureWorks-oltp\\Sales.SalesOrderHeader.parquet")
        product = spark.read.format("parquet").load(".\\resources\\parquet\\AdventureWorks-oltp\\Production.Product.parquet")
        sp = spark.read.format("parquet").load(".\\resources\\parquet\\AdventureWorks-oltp\\Sales.SalesPerson.parquet")
        person = spark.read.format("parquet").load(".\\resources\\parquet\\AdventureWorks-oltp\\Person.Person.parquet")

        # All sales
        sales = sod.join(soh, sod["SalesOrderID"] == soh["SalesOrderID"], "inner") \
            .join(product, sod["ProductID"] == product["ProductID"], "inner") \
            .join(sp, soh["SalesPersonID"] == sp["BusinessEntityID"], "left") \
            .join(person, soh["SalesPersonID"] == person["BusinessEntityID"], "left") \
            .withColumn("SalesPersonName"
                ,when(col("SalesPersonID").isNull(), "ONLINE")
                    .otherwise(concat(person["LastName"], lit(", "), person["FirstName"]))) \
            .withColumnRenamed("Name", "ProductName") \
            .select("SalesPersonName", "ProductName", sod["LineTotal"])

        # Group sales by person, product and sum LineTotal
        salesGrouped = sales.groupBy("SalesPersonName", "ProductName") \
            .agg(sum("LineTotal").alias("Revenue"))

        # Compute top products by revenue
        wdw = Window.partitionBy("SalesPersonName") \
            .orderBy(col("Revenue").desc())
        topProducts = salesGrouped.withColumn("Rank", dense_rank().over(wdw)) \
            .filter(col("Rank") <= 3) \
            .orderBy(col("SalesPersonName").asc(), col("Rank").asc())

        print("----------------------------------------------------------------------")
        print("  Product Margins")
        print("----------------------------------------------------------------------")
        topProducts.show()



    productByMargin()
    productBySalesPersonByQuantity()
    productBySalesPersonByRevenue()



    spark.stop()