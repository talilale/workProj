from utils import *
from pyspark.sql.functions import *
import os


def run(spark):
    try:
        dir_path = os.path.dirname(os.path.realpath(__file__))
        src_file = dir_path + '/data_folder/phaseTwo'
        tgt_file = dir_path + '/data_folder/phaseThree'

        df = spark.read.option("charset", "UTF-8").option("header",True).csv(src_file)

        # Extract values from ConsumptionTotalText
        df = df.withColumn("extracted-value-ConsumptionTotalText", substring_index("ConsumptionTotalText", " ", 1))
        df = df.withColumn("extracted-unit-ConsumptionTotalText", substring_index("ConsumptionTotalText", " ", -1))
        df = df.withColumn("extracted-unit-ConsumptionTotalText", regexp_replace("extracted-unit-ConsumptionTotalText", "/100","_"))
        df = df.withColumn("extracted-unit-ConsumptionTotalText", regexp_replace("extracted-unit-ConsumptionTotalText", "km","km_consumption"))
        # Save to csv file, repartitioning for easy view
        save_file(df, tgt_file)
        return True
    except Exception as e:
        return False
