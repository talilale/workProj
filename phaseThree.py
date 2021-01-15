from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os


def run():
    try:
        dir_path = os.path.dirname(os.path.realpath(__file__))
        src_file = dir_path + '/data_folder/phaseTwo'
        tgt_file = dir_path + '/data_folder/phaseThree'

        spark = SparkSession.builder.appName("phaseThree")\
        .config("spark.sql.shuffle.partitions", "50")\
        .config("spark.driver.maxResultSize","5g")\
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")\
        .getOrCreate()

        df = spark.read.option("charset", "UTF-8").option("header",True).csv(src_file)

        # Extract values from ConsumptionTotalText
        df = df.withColumn("extracted-value-ConsumptionTotalText", substring_index("ConsumptionTotalText", " ", 1))
        df = df.withColumn("extracted-unit-ConsumptionTotalText", substring_index("ConsumptionTotalText", " ", -1))
        df = df.withColumn("extracted-unit-ConsumptionTotalText", regexp_replace("extracted-unit-ConsumptionTotalText", "/100","_"))
        df = df.withColumn("extracted-unit-ConsumptionTotalText", regexp_replace("extracted-unit-ConsumptionTotalText", "km","km_consumption"))
        # Save to csv file, repartitioning for easy view
        df \
        .repartition(1)\
        .write.format("com.databricks.spark.csv")\
        .mode("overwrite")\
        .option("header", "true")\
        .option("charset", "UTF-8")\
        .save(tgt_file,format="csv")
        print(3)
        return 1
    except Exception as e:
        return 0
