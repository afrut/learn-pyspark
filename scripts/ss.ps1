Clear-Host
spark-submit --master local `
    --jars "D:\Spark\sqljdbc_10.2\enu\mssql-jdbc-10.2.0.jre8.jar" `
    DataFrameDemo.py `
    $Env:SPARK_HOME+"\README.md"