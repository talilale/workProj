from utils import *
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType
import os


def run(spark):
    try:
        dir_path = os.path.dirname(os.path.realpath(__file__))
        src_file = dir_path + '/data_folder/phaseThree'
        tgt_file = dir_path + '/data_folder/phaseFour'

        # Renaming columns and adding missing columns with null values
        df = spark.read.option("charset", "UTF-8").option("header",True).csv(src_file)
        df = df.withColumnRenamed("MakeText", "make")\
        .withColumnRenamed("ModelText", "model")\
        .withColumnRenamed("TypeName", "model_variant")\
        .withColumnRenamed("City", "city")\
        .withColumnRenamed("BodyTypeText", "carType")\
        .withColumnRenamed("ConditionTypeText", "condition")\
        .withColumnRenamed("TransmissionTypeText", "drive")\
        .withColumnRenamed("FirstRegYear", "manufacture_year")\
        .withColumnRenamed("Km", "mileage")\
        .withColumnRenamed("FirstRegMonth", "manufacture_month")\
        .withColumnRenamed("extracted-unit-ConsumptionTotalText", "fuel_consumption_unit")\
        .withColumn("currency", lit(None).cast(StringType()))\
        .withColumn("country", lit(None).cast(StringType()))\
        .withColumn("mileage_unit", lit(None).cast(StringType()))\
        .withColumn("price_on_request", lit(None).cast(StringType()))\
        .withColumn("type", lit(None).cast(StringType()))\
        .withColumn("zip", lit(None).cast(StringType()))

        df = df.select("carType","color","condition","currency","drive","city","country","make","manufacture_year","mileage","mileage_unit","model","model_variant","price_on_request","type","zip","manufacture_month","fuel_consumption_unit")

        # Save to csv file, repartitioning for easy view
        save_file(df, tgt_file)
        return True
    except Exception as e:
        return False
