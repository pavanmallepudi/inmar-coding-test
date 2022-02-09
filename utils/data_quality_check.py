# data quality checks module
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame,column
from pyspark.sql.functions import expr,when,regexp_replace,split,size,array_max,max,lit,col,trim,concat

def data_file_columns():
    columns_list=["url","address","name","rate","votes","phone","location","rest_type","dish_liked","cuisines","reviews_list", "Type_of_issue"]
    return columns_list

def is_not_null(input:DataFrame,colname:str,col_exist=True):
    if(col_exist == False):
        input=input.withColumn("Type_of_issue",lit(" "))
    output_df=input.withColumn("Type_of_issue", when(col(colname).isNull(), concat(col("Type_of_issue"),lit(","),lit(colname+" is Null"))))
    return output_df

def remove_special_charecters(input:DataFrame,colname:str):
    return input.withColumn(colname, regexp_replace(col(colname), "[^A-Z0-9_]", ""))

def is_valid_values(input:DataFrame,master:DataFrame,colname:str):
    op_df=input.alias("a").join(master.alias("b"), trim(input[colname])==trim(master[colname]), "left")\
        .selectExpr("a.*","b."+colname+" as refval") \
        .withColumn("Type_of_issue", when(col("refval").isNull(), concat(col("Type_of_issue"),lit(","),lit("Invalid"+colname))))\
        .drop("refval")

    return op_df


def validate_phone_numbers(input:DataFrame):
    input.printSchema()
    df2=input.withColumn("phone",regexp_replace(input.phone,'\\+91','')) \
        .withColumn("phone", regexp_replace(col('phone'), "[^0-9a-zA-Z]+", '')) \
        .withColumn("phone", regexp_replace(col('phone'), ' ', '')) \
        #.withColumn("phone",regexp_replace(col('phone'),"[\\[\\]]",""))
    #regexp_replace($"str", , "_")
    # df2.show(10,False)

    df2 = df2.withColumn("Type_of_issue", when(trim(col('phone')).cast("bigint").isNull(), lit("incorrect phone number")))\
        # .filter(col("Type_of_issue").isNull())\
        #
    # df2.show(40,False)
    # # If you don't know the number of columns:
    # df_sizes = df2.select(size('phone').alias('phone'))
    # df_max = df_sizes.agg(max('phone'))
    # nb_columns = df_max.collect()[0][0]
    #
    # df_result = df2.select(*columns,"Type_of_issue", *[df2['phone'][i] for i in range(nb_columns)])
    # df_result.printSchema
    # df_result.show()
    return df2


def expand_phone_numbers(input:DataFrame):
    cols=data_file_columns()
    cols.remove("phone")
    temp_df=input.select(*cols, split('phone', ',').alias('phone'))
    df_sizes = temp_df.select(size('phone').alias('phone'))
    df_max = df_sizes.agg(max('phone'))
    nb_columns = df_max.collect()[0][0]
    df_result = temp_df.select(*cols, "Type_of_issue", *[temp_df['phone'][i] for i in range(nb_columns)])
    return df_result

def apply_dq_checks(input:DataFrame,spark:SparkSession):
    ##apply null checks for name,phone,location
    temp_df=is_not_null(input,'name',False)
    temp_df=is_not_null(temp_df,'phone')
    temp_df=is_not_null(temp_df, 'name')
    ##phone numbers validation to remove junk
    temp_df=validate_phone_numbers(temp_df)
    ## remove junk from address,, reviewlist
    temp_df=remove_special_charecters(temp_df,'address')
    temp_df=remove_special_charecters(temp_df,'reviews_list')
    ##valid values check aeras
    aeras_master_df=spark.read.option("header","true").csv("lookup/Areas_in_blore.csv").withColumnRenamed("Area","location")  ## remove hard code
    temp_df=is_valid_values(temp_df,aeras_master_df,'location')
    temp_df=expand_phone_numbers(temp_df)
    temp_df.show(10,False)
    return temp_df



