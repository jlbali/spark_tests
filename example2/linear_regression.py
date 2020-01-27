from pyspark.sql import SparkSession



def test_LR() -> None:
    spark = SparkSession.builder.appName("lin_reg").getOrCreate()
    df = spark.read.csv("./datasets/Linear_regression_dataset.csv", inferSchema=True,
                        header = True)
    df.show()
    #print("Rows:", df.count()) # Generates a strange exception.
    print("Columns: ", len(df.columns))
    df.printSchema()
    print("Descriptive statistics")
    #df.describe() # Generates a strange exception.


