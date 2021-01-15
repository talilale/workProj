from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType
import os

def run():
    try:
        dir_path = os.path.dirname(os.path.realpath(__file__))
        src_file = dir_path + '/data_folder/phaseThree'
        tgt_file = dir_path + '/data_folder/phaseFour'

        spark = SparkSession.builder.appName("phaseFour")\
        .config("spark.sql.shuffle.partitions", "50")\
        .config("spark.driver.maxResultSize","5g")\
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")\
        .getOrCreate()

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

        dfbody = df.select("carType","color","condition","currency","drive","city","country","make","manufacture_year","mileage","mileage_unit","model","model_variant","price_on_request","type","zip","manufacture_month","fuel_consumption_unit")

        # Save to csv file, repartitioning for easy view
        dfbody \
        .repartition(1)\
        .write.format("com.databricks.spark.csv")\
        .mode("overwrite")\
        .option("header", "true")\
        .option("charset", "UTF-8")\
        .save(tgt_file,format="csv")
        print(4)
        return 1
    except Exception as e:
        return 0
