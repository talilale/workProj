from utils import *
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType
import os
import datetime as dt
import logging

now = str(dt.datetime.now().strftime('%Y%m%d%H%M%S'))
dir_path = os.path.dirname(os.path.realpath(__file__))


def run(spark, log_file):
    try:
        logging.getLogger(log_file).setLevel(logging.INFO)
        src_file = dir_path + '/data_folder/phaseThree'
        tgt_file = dir_path + '/data_folder/phaseFour'
        logging.info(now + ' Starting phase four, integration')

        # Renaming columns and adding missing columns with null values
        logging.info(now + ' P4: Reading phase file: ' + src_file)
        df = spark.read.option("charset", "UTF-8").option("header" ,True).csv(src_file)
        msg = ' P4: Source file - rows: ' + str(df.count()) + ' columns:' + str(len(df.columns))
        logging.info(now + msg)
        logging.info(now + ' P4: Adapting schema to target')
        df = df.withColumnRenamed("MakeText", "make") \
            .withColumnRenamed("ModelText", "model") \
            .withColumnRenamed("TypeName", "model_variant") \
            .withColumnRenamed("City", "city") \
            .withColumnRenamed("BodyTypeText", "carType") \
            .withColumnRenamed("ConditionTypeText", "condition") \
            .withColumnRenamed("TransmissionTypeText", "drive") \
            .withColumnRenamed("FirstRegYear", "manufacture_year") \
            .withColumnRenamed("Km", "mileage") \
            .withColumnRenamed("FirstRegMonth", "manufacture_month") \
            .withColumnRenamed("extracted-unit-ConsumptionTotalText", "fuel_consumption_unit") \
            .withColumn("currency", lit(None).cast(StringType())) \
            .withColumn("country", lit(None).cast(StringType())) \
            .withColumn("mileage_unit", lit(None).cast(StringType())) \
            .withColumn("price_on_request", lit(None).cast(StringType())) \
            .withColumn("type", lit(None).cast(StringType())) \
            .withColumn("zip", lit(None).cast(StringType()))

        df = df.select("carType","color","condition","currency","drive","city","country","make","manufacture_year","mileage","mileage_unit","model","model_variant","price_on_request","type","zip","manufacture_month","fuel_consumption_unit")

        # Save to csv file, repartitioning for easy view
        msg = ' P4: Target file - rows: ' + str(df.count()) + ' columns:' + str(len(df.columns))
        logging.info(now + msg)
        save_file(df, tgt_file)
        logging.info(now + ' P4: Completed')
        return True
    except Exception as e:
        logging.getLogger(log_file).setLevel(logging.ERROR)
        logging.error(now + str(e))
        return False
