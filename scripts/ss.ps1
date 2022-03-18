Clear-Host
spark-submit --master local `
    HelloWorld.py `
    $($Env:SPARK_HOME+"\README.md")