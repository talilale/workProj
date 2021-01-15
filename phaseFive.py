from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os


def run():
    try:
        dir_path = os.path.dirname(os.path.realpath(__file__))
        src_file = dir_path + '/data_folder/phaseFour'
        # For clear testing purposes the target file is not updated directly, but saved as new file
        tgt_file = dir_path + '/data_folder/Targer Data11.xlsx'
        #tgt_file = '/mnt/hgfs/VM/Targer Data11.xlsx'

        spark = SparkSession.builder.appName("phaseFive")\
        .getOrCreate()

        df = spark.read.option("charset", "UTF-8").option("header",True).csv('/home/mona/Data Engineer Task/phaseFour')
        df = df.distinct()
        tgt = pd.read_excel('/home/mona/Data Engineer Task/Target Data.xlsx', sheet_name='Sheet1', engine='openpyxl', na_values=[''],keep_default_na=False)

        mySchema = StructType([ StructField("carType", StringType(), True)\
                               ,StructField("color", StringType(), True)\
                               ,StructField("condition", StringType(), True)\
                               ,StructField("currency", StringType(), True)\
                               ,StructField("drive", StringType(), True)\
                               ,StructField("city", StringType(), True)\
                               ,StructField("country", StringType(), True)\
                               ,StructField("make", StringType(), True)\
                               ,StructField("manufacture_year", StringType(), True)\
                               ,StructField("mileage", StringType(), True)\
                               ,StructField("mileage_unit", StringType(), True)\
                               ,StructField("model", StringType(), True)\
                               ,StructField("model_variant", StringType(), True)\
                               ,StructField("price_on_request", StringType(), True)\
                               ,StructField("type", StringType(), True)\
                               ,StructField("zip", StringType(), True)\
                               ,StructField("manufacture_month", StringType(), True)\
                               ,StructField("fuel_consumption_unit", StringType(), True)])

        tgtdf = spark.createDataFrame(tgt,schema=mySchema)
        tgtdf = tgtdf.withColumn("manufacture_month", regexp_replace("manufacture_month", ".0",""))

        # Finding products that don't exist in target system
        merge_insert = df\
        .join(tgtdf, (df.color == tgtdf.color) & (df.city == tgtdf.city) & (df.make == tgtdf.make)\
              & (df.manufacture_year == tgtdf.manufacture_year) & (df.manufacture_month == tgtdf.manufacture_month),"left_anti")\
        .distinct()
        # Finding products that don't exist in source system
        merge_old = tgtdf\
        .join(df, (df.color == tgtdf.color) & (df.city == tgtdf.city) & (df.make == tgtdf.make)\
              & (df.manufacture_year == tgtdf.manufacture_year) & (df.manufacture_month == tgtdf.manufacture_month),"left_anti")\
        .distinct()
        # Finding rows that are to be updated. For demo I'm updating only mileage column
        merge_update = df\
        .join(tgtdf, (df.color == tgtdf.color) & (df.city == tgtdf.city) & (df.make == tgtdf.make)\
              & (df.manufacture_year == tgtdf.manufacture_year) & (df.manufacture_month == tgtdf.manufacture_month),"inner")\
            .select(tgtdf["*"],df["mileage"].alias("new_mileage"))
        merge_update = merge_update.withColumn("mileage", when(col("mileage")=='0.0',col("new_mileage")).otherwise(col("mileage")))
        merge_update = merge_update.drop(col("new_mileage"))
        merge_update = merge_update.distinct()
        # Union full target result
        full_result = merge_update.union(merge_insert).union(merge_old)

        target_result = full_result.toPandas()
        target_result.to_excel(tgt_file, index=None, header=True)
        print(5)
        return 1
    except Exception as e:
        return 0
