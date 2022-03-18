import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

if __name__ == '__main__':
    numArgs = len(sys.argv)
    if numArgs > 1:
        print("----------------------------------------------------------------------")
        print("  HelloWorld")
        print("----------------------------------------------------------------------")
        filepath = sys.argv[1]
        spark = SparkSession.builder.appName("HelloWorld").getOrCreate()

        # Load and cache.
        textFile = spark.read.format("text").load(filepath)
        #textFile = spark.read.text(filepath)
        df = textFile.cache()

        # Inspect.
        print(f"Number of lines: {df.count()}")
        print(f"First line: {df.first()}")

        # Filter and retrieve.
        lines = df["value"]
        linesSpark = df.filter(lines.contains("Spark"))
        numLinesSpark = linesSpark.count()
        numLinesA = df.filter(lines.contains("a")).count()
        numLinesB = df.filter(lines.contains("b")).count()
        print(f"Number of lines with \"Spark\": {numLinesSpark}")
        print(f"Number of lines with \"a\": {numLinesA}")
        print(f"Number of lines with \"b\": {numLinesB}")

        # Apply a function to every row in the DataFrame.
        splitfunc = lambda a : a.value.split(" ")
        wordsMap = df.rdd.map(splitfunc)
        wordsFlatMap = df.rdd.flatMap(splitfunc)

        # Find the nubmer of words in the line with the most words.
        numWordsPerLine = wordsMap.map(lambda ls: len(ls))
        numWordsMax = numWordsPerLine.reduce(max)
        print(f"Number of words in the line with most words: {numWordsMax}")

        # Group the data by their content(words).
        grouped = wordsFlatMap.map(lambda word: (word, 1)).groupByKey()
        wordCounts = grouped.mapValues(len)
        lsWordCounts = wordCounts.collect()
        for key, val in lsWordCounts:
            print(f'{key}: {val}')

        spark.stop()