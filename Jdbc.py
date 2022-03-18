from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName("DataFrameCreation").getOrCreate()

    # Get tablenames which are to be written to parquet files
    query = "select s.name + '.' + t.name TableName " \
            "from sys.tables t " \
            "join sys.schemas s " \
            "on t.schema_id = s.schema_id " \
            "where s.name != 'dbo'"
    dftn = spark.read.format("jdbc")\
        .option("url",  "jdbc:sqlserver://localhost:1433;databaseName=AdventureWorks;trustServerCertificate=true;integratedsecurity=true")\
        .option("query", query)\
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")\
        .load()
    tableNames = list(map(lambda row: row.TableName, dftn.collect()))

    # Query each table and write to parquet file.
    for tableName in tableNames:
        df = spark.read.format("jdbc")\
            .option("url",  "jdbc:sqlserver://localhost:1433;databaseName=AdventureWorks;trustServerCertificate=true;integratedsecurity=true")\
            .option("dbtable", tableName)\
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")\
            .load()
        df.write.format("parquet").mode("overwrite").save(f".\\parquet\\AdventureWorks-oltp\\{tableName}.parquet")

    spark.stop()