from utils import *
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os
import logging
import datetime as dt

now = str(dt.datetime.now().strftime('%Y%m%d%H%M%S'))
dir_path = os.path.dirname(os.path.realpath(__file__))


def run(spark, log_file):
    try:
        logging.getLogger(log_file).setLevel(logging.INFO)
        src_file = dir_path + '/data_folder/phaseFour'
        # For clear testing/review purposes the target file is not updated directly, but saved as new file
        tgt_file = dir_path + '/data_folder/Targer Data11.xlsx'
        logging.info(now + ' Starting phase five, merging')

        logging.info(now + ' P5: Reading phase file: ' + src_file)
        df = spark.read.option("charset", "UTF-8").option("header",True).csv(src_file)
        msg = ' P5: Source file - rows: ' + str(df.count()) + ' columns:' + str(len(df.columns))
        logging.info(now + msg)

        tgt = pd.read_excel('/home/mona/Data Engineer Task/Target Data.xlsx', sheet_name='Sheet1', engine='openpyxl', na_values=[''],keep_default_na=False)

        my_schema = StructType([ StructField("carType", StringType(), True)\
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

        logging.info(now + ' P5: Reading and simple processing of target file')
        tgtdf = spark.createDataFrame(tgt,schema=my_schema)
        tgtdf = tgtdf.withColumn("manufacture_month", regexp_replace("manufacture_month", ".0",""))

        # Finding products that don't exist in target system
        logging.info(now + ' P5: Processing data that does not exist in target')
        merge_insert = df\
        .join(tgtdf, (df.color == tgtdf.color) & (df.city == tgtdf.city) & (df.make == tgtdf.make)\
              & (df.manufacture_year == tgtdf.manufacture_year) & (df.manufacture_month == tgtdf.manufacture_month),"left_anti")\
        .distinct()
        msg = ' P5: Not in target - rows: ' + str(merge_insert.count()) + ' columns:' + str(len(merge_insert.columns))
        logging.info(now + msg)
        # Finding products that don't exist in source system
        logging.info(now + ' P5: Processing data that does not exist in source')
        merge_old = tgtdf \
            .join(df, (df.color == tgtdf.color) & (df.city == tgtdf.city) & (df.make == tgtdf.make) \
                  & (df.manufacture_year == tgtdf.manufacture_year) & (df.manufacture_month == tgtdf.manufacture_month),"left_anti") \
            .distinct()
        msg = ' P5: Not in source - rows: ' + str(merge_old.count()) + ' columns:' + str(len(merge_old.columns))
        logging.info(now + msg)
        # Finding rows that are to be updated. For demo I'm updating only mileage column
        logging.info(now + ' P5: Processing data that will do only updates in target')
        merge_update = df \
            .join(tgtdf, (df.color == tgtdf.color) & (df.city == tgtdf.city) & (df.make == tgtdf.make) \
                  & (df.manufacture_year == tgtdf.manufacture_year) & (df.manufacture_month == tgtdf.manufacture_month),"inner") \
            .select(tgtdf["*"],df["mileage"].alias("new_mileage"))
        merge_update = merge_update.withColumn("mileage", when(col("mileage")=='0.0',col("new_mileage")).otherwise(col("mileage")))
        merge_update = merge_update.drop(col("new_mileage"))
        merge_update = merge_update.distinct()
        msg = ' P5: To be updated - rows: ' + str(merge_update.count()) + ' columns:' + str(len(merge_update.columns))
        logging.info(now + msg)
        # Union full target result
        logging.info(now + ' P5: Merging')
        full_result = merge_update.union(merge_insert).union(merge_old)

        msg = ' P5: Target file full data - rows: ' + str(full_result.count()) + ' columns:' + str(len(full_result.columns))
        logging.info(now + msg)
        target_result = full_result.toPandas()
        target_result.to_excel(tgt_file, index=None, header=True)
        logging.info(now + ' P5: Completed')
        return True
    except Exception as e:
        logging.getLogger(log_file).setLevel(logging.ERROR)
        logging.error(now + str(e))
        return False
