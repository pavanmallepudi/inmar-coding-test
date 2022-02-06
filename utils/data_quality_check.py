# data quality checks module

from pyspark.sql import DataFrame,column
from pyspark.sql.functions import expr,when,regexp_replace,split,size,array_max,max,lit,col,trim

def data_file_columns():
    columns_list=["url","address","name","rate","votes","phone","location","rest_type","dish_liked","cuisines","reviews_list"]
    return columns_list

def is_not_null(input:DataFrame,colname:str):
    output_df=input.withColumn("Type_of_issue", when(input[colname].isNull, "Null"))
    return output_df

def remove_special_charecters(input:DataFrame,colname:str):
    return input.withColumn(colname, regexp_replace(input.col(colname), "[^A-Z0-9_]", ""))

def expand_phone_numbers(input:DataFrame):
    columns=data_file_columns()
    columns.remove("phone")
    print(columns)
    input.printSchema
    df2=input.withColumn("phone",regexp_replace(input.phone,'\\+91','')) \
    #.withColumn("phone", regexp_replace(input.phone, ' ', '')) \
    #

    df2 = df2.withColumn("Type_of_issue", when(trim(col('phone')).cast("int").isNull(), lit("incorrect phone numer"))).filter(col('Type_of_issue')!=lit("incorrect phone numer")).select(*columns, split('phone', ',').alias('phone'))

    # If you don't know the number of columns:
    df_sizes = df2.select(size('phone').alias('phone'))
    df_max = df_sizes.agg(max('phone'))
    nb_columns = df_max.collect()[0][0]

    df_result = df2.select(*columns, *[df2['phone'][i] for i in range(nb_columns)])
    df_result.printSchema
    df_result.show()
    return df_result