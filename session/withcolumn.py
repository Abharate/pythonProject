from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("With Column").getOrCreate()

    schema_data = StructType([
        StructField("id",IntegerType()),
        StructField("name",StringType()),
        StructField("gender",StringType()),
        StructField("city",StringType()),
        StructField("salary",DoubleType())
    ])

    df = spark.read.load(r"C:\Users\Sunny\PycharmProjects\pythonProject\session\employee.csv",
                         format="csv",schema=schema_data)

    df.printSchema()
    df.show()

    # Change datatype of existing column

    df1 = df.withColumn("salary",col("salary").cast("Integer")).printSchema()
    df1.show()
    df1.printSchema()

    df.withColumn("salary",col("salary")*100).show()

    # assignment
    # add new column "state" which will have static value as "MH"

    # rename column
    df.withColumnRenamed("name","first_Name").printSchema()

    df.drop("city").printSchema()

