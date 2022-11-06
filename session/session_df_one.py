from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.config('spark.port.maxRetries', 100).getOrCreate()
    spark = SparkSession.builder.master("local[0]").appName("Dataframe Session One").getOrCreate()
    #print(spark)

    data = [("Ram","Male"),("Shyam","Male"),("Kiran","Female")]
    header = ["name", "gender"]

    #create dataFrame from existing RDD
    input_rdd = spark.sparkContext.parallelize(data)
    # input_df = input_rdd.toDF()
    # input_df.show()
    #print(input_rdd.collect())

    # create dataFrame from existing RDD with Header
    # input_df1 = input_rdd.toDF(header)
    # input_df1.show()
    # input_df1.printSchema()

    # Create DataFrame
    # input_df2 = spark.createDataFrame(input_rdd,schema=header)
    # input_df2.show()

    # Create DataFrame from Input File
    csv_df = spark.read.csv(r"C:\Users\Sunny\PycharmProjects\pythonProject\session\employee.csv",
                            schema= "id int, name string, age int, salary int")
    csv_df.show()
    csv_df.printSchema()
