
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from utils.file_check import fetch_file_list_from_directory,last_processed_date
from utils.data_quality_check import apply_dq_checks
from utils.output_files import write_correct_files,write_incorrect_files,extract_error_meta_data


def main():
    spark = SparkSession.builder.appName('demo').master('local[2]').getOrCreate()

    landing_zone="input/"

    raw_df = fetch_file_list_from_directory(spark,landing_zone,last_processed_date())
    dq_check_df=apply_dq_checks(raw_df,spark)

    write_correct_files(dq_check_df)
    write_incorrect_files(dq_check_df)
    extract_error_meta_data(dq_check_df)

    meta_data=spark.read.parquet("output/incorrect_data_metadata")

if __name__ == "__main__":
    main()
