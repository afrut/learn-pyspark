Clear-Host
if($args.Length -gt 0)
{
    spark-submit --master local `
        --jars "D:\Spark\sqljdbc_10.2\enu\mssql-jdbc-10.2.0.jre8.jar" `
        $args[0] `
        $($Env:SPARK_HOME + "\README.md")
}