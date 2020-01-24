
from pyspark.sql import SparkSession

def test1() -> None:
    spark = SparkSession.builder.appName("Basics").getOrCreate()
    df = spark.read.json("./datasets/people.json")
    df.show()
    df.printSchema()
    df.columns
    df.describe().show()

