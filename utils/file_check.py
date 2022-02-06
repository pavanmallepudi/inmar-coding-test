# file check module

from os.path import exists
import os
import glob
import time
from pyspark.sql import SparkSession


def is_a_csv(path: str):
    ext = os.path.splitext(path)[-1].lower()
    if ext == ".csv":
        return True
    return False


def is_empty_file(path: str):
    return os.stat(path).st_size == 0


def is_file_processed(path: str, last_processed_date: str):
    date_part=path.split("_")[2][0:8]
    print("date_part    "+date_part)
    lt_processed_dt = time.strptime(last_processed_date, "%Y%m%d")
    file_upload_time = time.strptime(date_part, "%Y%m%d")
    return file_upload_time > lt_processed_dt


def fetch_file_list_from_directory(spark: SparkSession, path: str, last_processed_date):
    files_list = []
    for (root, dirs, file) in os.walk(path):
        for f in file:
            file_path=root+f
            print(file_path)
            if not (is_empty_file(file_path)):
                if is_a_csv(file_path):
                    if is_file_processed(file_path, last_processed_date):
                        files_list.append(file_path)
    print(files_list)
    return spark.read.option("header", "true").csv(files_list)


def last_processed_date():
    return "20210526"
