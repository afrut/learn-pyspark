$path = @(
    "C:\WINDOWS\system32",
    "C:\Program Files\PowerShell\7\",
    "D:\Python\Python39\Scripts\",
    "D:\Python\Python39\",
    "D:\Program Files\Git\cmd",
    "D:\Spark\spark-3.0.3-bin-hadoop2.7\bin",
    "D:\Spark\spark-3.0.3-bin-hadoop2.7\sbin",
    "D:\Spark\hadoop-2.7.1\bin",
    "D:\src\powershell-scripts\git\",
    ".\scripts\",
    "D:\Spark\sqljdbc_10.2\enu\auth\x64\",
    "D:\scripts"
)

$Env:PATH=$path -join ";"
$Env:JAVA_HOME="C:\Program Files\Java\jdk1.8.0_202"
$Env:SPARK_HOME="D:\Spark\spark-3.0.3-bin-hadoop2.7"
$Env:HADOOP_HOME="D:\Spark\hadoop-2.7.1"
$Env:KAFKA_HOME="/mnt/d/kafka_2.13-3.1.0"
$Env:SPARK_EXAMPLES=$Env:SPARK_HOME + "examples\src\main"