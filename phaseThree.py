from utils import *
from pyspark.sql.functions import *
import os
import logging
import datetime as dt

now = str(dt.datetime.now().strftime('%Y%m%d%H%M%S'))
dir_path = os.path.dirname(os.path.realpath(__file__))


def run(spark, log_file):
    try:
        logging.getLogger(log_file).setLevel(logging.INFO)
        src_file = dir_path + '/data_folder/phaseTwo'
        tgt_file = dir_path + '/data_folder/phaseThree'
        logging.info(now + ' Starting phase three, extraction')

        logging.info(now + ' P3: Reading phase file: ' + src_file)
        df = spark.read.option("charset", "UTF-8").option("header", True).csv(src_file)
        msg = ' P3: Source file - rows: ' + str(df.count()) + ' columns:' + str(len(df.columns))
        logging.info(now + msg)

        # Extract values from ConsumptionTotalText
        logging.info(now + ' P3: Extracting columns, splitting values')
        df = df.withColumn("extracted-value-ConsumptionTotalText", substring_index("ConsumptionTotalText", " ", 1))
        df = df.withColumn("extracted-unit-ConsumptionTotalText", substring_index("ConsumptionTotalText", " ", -1))
        df = df.withColumn("extracted-unit-ConsumptionTotalText", regexp_replace("extracted-unit-ConsumptionTotalText", "/100", "_"))
        df = df.withColumn("extracted-unit-ConsumptionTotalText", regexp_replace("extracted-unit-ConsumptionTotalText", "km", "km_consumption"))

        # Save to csv file, repartitioning for easy view
        msg = ' P3: Target file - rows: ' + str(df.count()) + ' columns:' + str(len(df.columns))
        logging.info(now + msg)
        save_file(df, tgt_file)
        logging.info(now + ' P3: Completed')
        return True
    except Exception as e:
        logging.getLogger(log_file).setLevel(logging.ERROR)
        logging.error(now + str(e))
        return False
