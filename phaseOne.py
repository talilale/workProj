from utils import *
from pyspark.sql.functions import first
import os
import logging
import datetime as dt

now = str(dt.datetime.now().strftime('%Y%m%d%H%M%S'))
dir_path = os.path.dirname(os.path.realpath(__file__))


def run(spark, log_file):
    try:
        logging.getLogger(log_file)
        src_file = dir_path + '/data_folder/supplier_car.json'
        tgt_file = dir_path + '/data_folder/phaseOne'
        logging.info(now + ' Starting phase one, pre-processing')

        # Grouping and pivoting based on attributes
        logging.info(now + ' P1: Reading source file: ' + src_file)
        df = spark.read.option("charset", "UTF-8").json(src_file)
        msg = ' P1: Source file - rows: ' + str(df.count()) + ' columns:' + str(len(df.columns))
        logging.info(now + msg)
        logging.info(now + ' P1: Pivoting rows')
        df = df.groupBy("ID", "MakeText", "ModelText", "ModelTypeText", "TypeName", "TypeNameFull").pivot('Attribute Names').agg(first('Attribute Values').alias('Attribute Values'))

        # Save to csv file, repartitioning for easy view
        msg = ' P1: Target file - rows: ' + str(df.count()) + ' columns:' + str(len(df.columns))
        logging.info(now + msg)
        save_file(df, tgt_file)
        logging.info(now + ' P1: Completed')
        return True
    except Exception as e:
        logging.getLogger(log_file).setLevel(logging.ERROR)
        logging.error(now + str(e))
        return False
