from pyspark.sql import SparkSession
from pyspark.sql.functions import *
if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("Salary").getOrCreate()

    data = [(1,"Ram",25,20000),(2,"Sham",29,30000),(3,"Kiran",31,10000),(4,"Mina",34,14000)]
    header = ["id","Name","Age","Salary"]

    input_rdd = spark.sparkContext.parallelize(data)
    input_df = input_rdd.toDF(header)
    input_df.show()

    # input_df.select("Salary").max.show()
    # max(input_df.Salary).show()
    input_df.withColumn("Salary",max_by("Salary",desc)).show()






