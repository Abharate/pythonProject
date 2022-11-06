from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import col

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("Dataframe session").getOrCreate()

    input_data = [Row(id=1,name="Ram",address=Row(city="Pune"),state="MH"),
                  Row(id=2,name="Sham",address=Row(city="Mumbai"),state="MH")]

    df = spark.createDataFrame(input_data)
    df.show()

    df.select("id").show()
    df.select(df.address.city).show()
    df.select(df["address.city"]).show()
    df.select(col("address.city")).show()

    df.select(col("address.*")).show()

    df1 = df.select(col("address.*"))
    print(df1)


