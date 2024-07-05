import sys
import pandas as pd
from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("PythonDataframe")\
        .getOrCreate()

    # create a dataframe
    data = {'name': ['John', 'Smith', 'Sarah'],
            'age': [19, 23, 18]}
    localDF = pd.DataFrame(data)

    # convert dataframe to a Spark dataframe
    df = spark.createDataFrame(localDF)

    # print its schema
    df.printSchema()

    # create a Dataframe from a JSON file
    peopleDF = spark.read.json("examples/src/main/resources/people.json")
    peopleDF.printSchema()

    # register dataframe as a table
    peopleDF.createOrReplaceTempView("people")

    # sql statements
    teenagers = spark.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

    # call collect
    teenagersLocalDF = teenagers.collect()

    # print the df
    print(teenagersLocalDF)

    spark.stop()
