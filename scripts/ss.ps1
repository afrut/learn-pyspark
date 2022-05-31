Clear-Host
$packages = @(
    "io.delta:delta-core_2.12:0.7.0"        # to use Delta Lake
)

$jars = @(
    "D:\Spark\sqljdbc_10.2\enu\mssql-jdbc-10.2.0.jre8.jar"      # jdbc driver to communicate with SQL Server
)

if($args.Length -gt 0)
{
    spark-submit --master local `
        --packages @packages `
        --jars @jars `
        @args
        #$($Env:SPARK_HOME + "\README.md")
}