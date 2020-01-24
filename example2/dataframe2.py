from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, avg, stddev
from pyspark.sql.functions import format_number

def testAgg_1() -> None:
    spark = SparkSession.builder.appName("aggregation").getOrCreate()
    df = spark.read.csv("./datasets/sales_info.csv", inferSchema=True, header=True)
    df.printSchema()
    df.show()
    print("Sales average")
    df.groupBy("Company").mean().show()
    print("Count")
    df.groupBy("Company").count().show()
    print("Aggregation")
    df.agg({"Sales": "sum"}).show()
    df.agg({"Sales": "max"}).show()
    print("Group and aggregation")
    group_data = df.groupBy("Company")
    group_data.agg({"Sales": "max"}).show()
    print("Functions")
    df.select(countDistinct("Sales")).show()
    df.select(avg("Sales")).show()
    df.select(avg("Sales").alias("Average Sales")).show()
    print("Format")
    sales_std = df.select(stddev("Sales").alias("std"))
    sales_std.select(format_number("std",2).alias("Standard Deviation")).show()
    print("Order By")
    df.orderBy("Sales").show() # Ascending order.
    df.orderBy(df["Sales"].desc()).show() # Descending order.


# Ejemplos con SQL.
def testAgg_2() -> None:
    spark = SparkSession.builder.appName("aggregation").getOrCreate()
    df = spark.read.csv("./datasets/sales_info.csv", inferSchema=True, header=True)
    df.createOrReplaceTempView("sales_table")  # Registering as SQL Temporal View.
    spark.sql("SELECT Company,avg(Sales) from sales_table GROUP BY Company").show()

