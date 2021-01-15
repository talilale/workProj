from pyspark.sql import SparkSession
from pyspark.sql.functions import first
import os


def run():
    try:
        dir_path = os.path.dirname(os.path.realpath(__file__))
        src_file = dir_path + '/data_folder/supplier_car.json'
        tgt_file = dir_path + '/data_folder/phaseOne'

        spark = SparkSession.builder.appName("phaseOne")\
        .config("spark.sql.shuffle.partitions", "50")\
        .config("spark.driver.maxResultSize","5g")\
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")\
        .getOrCreate()

        # Grouping and pivoting based on attributes
        df = spark.read.option("charset", "UTF-8").json(src_file)
        df1 = df.select("ID").distinct()
        dfbody = df
        dfbody = dfbody.groupBy("ID", "MakeText", "ModelText", "ModelTypeText", "TypeName", "TypeNameFull").pivot('Attribute Names').agg(first('Attribute Values').alias('Attribute Values'))

        # Save to csv file, repartitioning for easy view
        dfbody \
        .repartition(1)\
        .write.format("com.databricks.spark.csv")\
        .mode("overwrite")\
        .option("header", "true")\
        .option("charset", "UTF-8")\
        .save(tgt_file,format="csv")
        print(1)
        return 1
    except Exception as e:
        return 0
