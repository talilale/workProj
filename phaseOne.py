from utils import *
from pyspark.sql.functions import first
import os


def run(spark):
    try:
        dir_path = os.path.dirname(os.path.realpath(__file__))
        src_file = dir_path + '/data_folder/supplier_car.json'
        tgt_file = dir_path + '/data_folder/phaseOne'

        # Grouping and pivoting based on attributes
        df = spark.read.option("charset", "UTF-8").json(src_file)
        df = df.groupBy("ID", "MakeText", "ModelText", "ModelTypeText", "TypeName", "TypeNameFull").pivot('Attribute Names').agg(first('Attribute Values').alias('Attribute Values'))

        # Save to csv file, repartitioning for easy view
        save_file(df, tgt_file)
        return True
    except Exception as e:
        return False
