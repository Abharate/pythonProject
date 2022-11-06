from pyspark.sql import SparkSession

if __name__=='__main__':
    spark = SparkSession.builder.master("local[*]").appName("Row Class").getOrCreate()

    input_data = [Row(id=1,name="Ram",address=Row(city="Pune"),state="MH")),
                  Row(id=2,name="Sham",address=Row(city="Mumbai"),state="MH"))]

    df = spark.createDataFrame(input_data)
    df.show()
    df.printSchema()

