Clear-Host
$packages = @(
    "io.delta:delta-core_2.12:0.7.0"        # to use Delta Lake
) -join ","

$jars = @(
    "D:\Spark\sqljdbc_10.2\enu\mssql-jdbc-10.2.0.jre8.jar"      # jdbc driver to communicate with SQL Server
) -join ","

# Configuration description
# "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" is needed to use Delta Lake
# "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" is needed to use Delta Lake
# "spark.sql.shuffle.partitions=5" is used due to the scripts being run on a single local machine
# "spark.sql.streaming.schemaInference=true" is used to automatically infer schema when running streaming sources

if($args.Length -gt 0)
{
    spark-submit --master local `
        --packages $packages `
        --jars $jars `
        --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" `
        --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" `
        --conf "spark.sql.shuffle.partitions=5" `
        --conf "spark.sql.streaming.schemaInference=true" `
        @args
}