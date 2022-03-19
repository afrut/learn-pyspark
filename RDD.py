from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from operator import add

if __name__ == '__main__':
    spark = SparkSession.builder.appName("DataFrameCreation").getOrCreate()

    print("----------------------------------------------------------------------")
    print("  RDD Demo")
    print("----------------------------------------------------------------------")
    # Load some data
    filepathParquet = ".\\resources\\parquet\\AdventureWorks-oltp\\Sales.SalesOrderDetail.parquet"
    filepathText = ".\\resources\\txt\\a-scandal-in-bohemia-words-only.txt"
    dfp = spark.read.format("parquet").load(filepathParquet).cache()
    rdd = spark.sparkContext.textFile(filepathText).cache()

    # When the rdd is created from a DataFrame, each row is of type Row.
    print(f"Type of rdd row from DataFrame: {type(dfp.rdd.collect()[0])}")
    print(f"Type of rdd row from textFile(): {type(rdd.collect()[0])}")
    print('')

    # To convert a raw rdd to a DataFrame, a schema must be specified.
    # Method 1: Use a function to create Row objects.
    rdd.map(Row).toDF() # Default schema is _1: string (nullable = true)
    def makeColNames(x): return {"value": x} # Function to return a dict with column name "value"
    def createRow(x): return Row(**makeColNames(x)) # Function to create row object by expanding {column: row value} key-value pairs in dict
    rdd.map(createRow).toDF() # Expand key-value pairs of makerow as arguments to create a Row object with column names

    # Method 2: Use spark.createDataFrame with a specified schema.
    schema = StructType([StructField("value", StringType(), True)])
    spark.createDataFrame(rdd, schema)

    # Count number of words in each line
    # Create an rdd with tuples (value, index) where index is incrementing
    # Make the incrementing index the key to the rdd
    # Split the string value in every row with space
    # Make empty lines contain no strings instead of zero length strings
    wordsPerLine = rdd.zipWithIndex() \
        .map(lambda tpl: (tpl[1], tpl[0])) \
        .mapValues(lambda x: x.split(" ")) \
        .mapValues(lambda x: x) \
        .mapValues(lambda x: [] if x[0] == '' else x)

    # Count number of rows in every row
    wordsPerLineCount = wordsPerLine.mapValues(len)

    # Count total number of words
    totalWords = wordsPerLineCount.map(lambda tpl: tpl[1]).reduce(lambda a, b: a + b)
    allWords = rdd.flatMap(str.split)
    totalWords2 = allWords.count()

    # Frequency count of each word
    # Create tuple of (word, 1) with word as key
    # Add the value of every tuple grouped by key
    # Sort results by count of every word
    # Take top 10 most frequent words
    wordCount = allWords.map(lambda x: (x, 1)) \
        .reduceByKey(add) \
        .sortBy(lambda x: x[1], ascending = False) \
        .map(lambda x: (x[0], x[1], x[1]/totalWords)) \
        .take(10)

    assert(totalWords == totalWords2)
    print('Top 10 most frequent words:')
    print(f"    |----------|----------|----------|")
    print(f"    |{('word'):^10}|{('num'):^10}|{('pct'):^10}|")
    print(f"    |----------|----------|----------|")
    for word, num, pct in wordCount:
        print(f"    |{(word):^10}|{num:^10}|{pct:^10.2%}|")
    print(f"    |----------|----------|----------|")



    # Transform and reduce 1 column of a DataFrame
    dfp.rdd.map(lambda x: x["LineTotal"] / 1000) \
        .reduce(lambda a, b: a + b)



    spark.stop()