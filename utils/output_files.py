from pyspark.sql import SparkSession
from pyspark.sql import DataFrame,Window
from pyspark.sql.functions import col,trim,monotonically_increasing_id,split,explode,collect_set,lit,row_number




def write_correct_files(input:DataFrame):
    input.filter(trim(col("Type_of_issue")).isNull()).drop("Type_of_issue").write.option("header","true").mode("overwrite").parquet("output/correct_data.out/")

def write_incorrect_files(input:DataFrame):
    input.filter(trim(col("Type_of_issue")).isNotNull()).drop("Type_of_issue").write.option("header","true").mode("overwrite").parquet("output/incorrect_data.bad/")

def extract_error_meta_data(input:DataFrame):
    temp_df=input.select("Type_of_issue").withColumn("row_Id_no", row_number().over(Window.partitionBy(lit(1)).orderBy(lit(1))))
    temp_df=temp_df.filter(trim(col("Type_of_issue")).isNotNull()).select(col("row_Id_no"),explode(split('Type_of_issue', ',')).alias("Reason"))
    temp_df.groupby("Reason").agg(collect_set("row_Id_no").alias("row_numbers_list")).write.option("header","true").mode("overwrite").parquet("output/incorrect_data_metadata/")