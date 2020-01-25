from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, avg, stddev
from pyspark.sql.functions import format_number
from pyspark.sql.functions import mean
from pyspark.sql.functions import dayofmonth, hour, dayofyear, month, year, weekofyear,date_format

from pyspark.sql.dataframe import DataFrame

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



# Missing data.
def test_miss1() -> None:
    spark = SparkSession.builder.appName("miss").getOrCreate()
    df = spark.read.csv("./datasets/ContainsNull.csv", inferSchema=True, header=True)
    df.show()
    df.printSchema()
    print("Drop all with nulls")
    df.na.drop().show()
    print("Threshold of nulls")
    df.na.drop(thresh=2).show()
    print("How of nulls")
    df.na.drop(how="all").show()
    print("Nulls on Sales")
    df.na.drop(subset=["Sales"]).show()
    print("Filling missing values")
    df.na.fill("FILL VALUE").show()
    print("Fill by number")
    df.na.fill(0).show()
    print("Fill only name")
    df.na.fill("No Name", subset=["Name"]).show()
    print("Fill by means")
    mean_val = df.select(mean(df["Sales"])).collect()
    mean_sales = mean_val[0][0]
    print("Mean sales ", mean_sales)
    df.na.fill(mean_sales, ["Sales"]).show()


def test_timestamp() -> None:
    spark = SparkSession.builder.appName("dates").getOrCreate()
    df = spark.read.csv("./datasets/appl_stock.csv", header=True, inferSchema=True)
    print(df.head(1))
    df.select(["Date", "Open"]).show()
    df.select(dayofmonth(df["Date"])).show()
    df.select(hour(df["Date"])).show()
    # Obtain average closing value per year.
    df.select(year(df["Date"])).show()
    df.withColumn("Year", year(df["Date"])).show()
    new_df = df.withColumn("Year", year(df["Date"]))
    new_df.groupBy("Year").mean().select(["Year", "avg(Close)"]).show()
    result: DataFrame = new_df.groupBy("Year").mean().select(["Year", "avg(Close)"])
    # We specify the type so PyCharm autocomplete can handle it.
    print("Type of Result: ", type(result))
    result = result.withColumnRenamed("avg(Close)", "Average Closing Price")
    result.show()
    result = result.select(["Year", format_number("Average Closing Price",2).alias("Avg Close")])
    result = result.orderBy("Year")
    result.show()
