
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from utils.file_check import fetch_file_list_from_directory,last_processed_date
from utils.data_quality_check import data_file_columns,expand_phone_numbers


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


    #inputpath = "file:///C://Users//samme//Downloads//data_file_20210527182730.csv"
    #inputpath = "C:\Users\samme\Downloads\Areas_in_blore.xlsx"
    #inputpath = "input//data_file_20210527182730.csv"
    landing_zone="input/"

    raw_df = fetch_file_list_from_directory(spark,landing_zone,last_processed_date())

    trimed_df=raw_df.select(*data_file_columns())


    trimed_df.printSchema()


    trimed_df.show(20,False)

    df=expand_phone_numbers(trimed_df)

