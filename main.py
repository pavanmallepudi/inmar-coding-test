
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

if __name__ == "__main__":
    print("Application Started ...")

    spark = SparkSession.builder.appName('demo').master('local[2]').getOrCreate()

    valid_schema = StructType([ \
        StructField("url", StringType(), True), \
        StructField("address", StringType(), True), \
        StructField("name", StringType(), False), \
        StructField("rate", StringType(), True), \
        StructField("votes", StringType(), True), \
        StructField("phone", IntegerType(), True), \
        StructField("location", StringType(), True), \
        StructField("rest_type", StringType(), True), \
        StructField("dish_liked", IntegerType(), True), \
        StructField("cuisines", StringType(), True), \
        StructField("reviews_list", IntegerType(), True)
        ])

    inputpath = "file:///C://Users//samme//Downloads//data_file_20210527182730.csv"
    #inputpath = "C:\Users\samme\Downloads\Areas_in_blore.xlsx"

    raw_df = spark.read.option("header","true").csv(inputpath)


    raw_df.printSchema()


    raw_df.show(2,False)