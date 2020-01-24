
from pyspark.sql import SparkSession

from pyspark.sql.types import StructField, StringType, IntegerType, StructType



def test1() -> None:
    spark = SparkSession.builder.appName("Basics").getOrCreate()
    df = spark.read.json("./datasets/people.json")
    df.show()
    df.printSchema()
    df.columns
    df.describe().show()

def test2() -> None:
    data_schema = [StructField("age", IntegerType(), True),
                   StructField("name", StringType(), True)]
    final_struct = StructType(fields=data_schema)
    spark = SparkSession.builder.appName("Basics").getOrCreate()
    df = spark.read.json("./datasets/people.json", schema=final_struct)
    df.printSchema()

def test3() -> None:
    spark = SparkSession.builder.appName("Basics").getOrCreate()
    df = spark.read.json("./datasets/people.json")
    age = df.select('age')
    age.show()
    print(df.head(2)[0])
    df.select(["age", "name"]).show()
    df.withColumn("double_age", df["age"]*2).show() # Not an inplace operation, is a new dataframe.
    df.withColumnRenamed("age","new_age").show() # Not an inplace operation, is a new dataframe.
    df.show()

def test4() -> None:
    spark = SparkSession.builder.appName("Basics").getOrCreate()
    df = spark.read.json("./datasets/people.json")
    df.createOrReplaceTempView("people") # Registering as SQL Temporal View.
    results = spark.sql("SELECT * from people")
    results.show()
    results2 = spark.sql("SELECT * from people WHERE age > 25")
    results2.show()

def test5() -> None:
    spark = SparkSession.builder.appName("ops").getOrCreate()
    df = spark.read.csv("./datasets/appl_stock.csv", inferSchema=True, header=True)
    df.printSchema()
    df.show()
    # SQL way of filtering.
    df.filter("Close < 200").show()
    df.filter("Close < 200").select("Date").show()
    # Pandas way of filtering.
    df.filter(df["Close"] < 200).show()
    # Multiple 'and' Filters.
    print("Filtered by two conditions")
    #df.filter(df["Close"] < 200 & df["Open"] > 200).show() # Throws a rather obscure error.
    df.filter( (df["Close"] < 200) & (df["Open"] > 200)).show()
    # NOT operation.
    print("NOT operation")
    df.filter(~(df["Close"] < 200) & (df["Open"] > 200)).show()
    # Collect.
    print("Collect operation")
    result = df.filter("Close < 500").collect()
    print(result)
    row = result[0].asDict()
    print(row)