
from pyspark.sql import SparkSession

def test1():
    spark = SparkSession.builder.appName("Basics").getOrCreate()



