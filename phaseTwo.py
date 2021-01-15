from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as f
import os


def run():
    try:
        dir_path = os.path.dirname(os.path.realpath(__file__))
        src_file = dir_path + '/data_folder/phaseOne'
        tgt_file = dir_path + '/data_folder/phaseTwo'
        code_file = dir_path + '/data_folder/code_color.csv'

        spark = SparkSession.builder.appName("phaseTwo")\
        .config("spark.sql.shuffle.partitions", "50")\
        .config("spark.driver.maxResultSize","5g")\
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")\
        .getOrCreate()

        df = spark.read.option("charset", "UTF-8").option("header",True).csv(src_file)
        # Created a code file for colors based on distinct value from source file and entered translations based on target table.
        # Could use a translator api or just prepare a dict. In normal environment I'd create a code/mapping files/tables/dict
        # for every column that are element types and there is a difference in naming between source and target.
        dfColor = spark.read.option("charset", "UTF-8").option("header",True).option("delimiter", ";").csv(code_file)
        dfColorbody = dfColor
        # Target table doesnt differentiate on mat/metallic so cutting color only
        dfbody = df.withColumn("BodyColorText", substring_index("BodyColorText", " ", 1))
        # Joining main dataframe with color dataframe, removing unnecessary columns, switching translation column name,
        # performing additional operations to initcap car names and fixing some big car brands, also should be mapped.
        newbody = dfbody.join(dfColorbody, dfbody.BodyColorText == dfColorbody.german, how = 'left')\
           .drop("BodyColorText", "german")\
           .withColumnRenamed("english", "color")\
           .withColumn("MakeText", f.trim("MakeText"))\
           .withColumn("MakeText", initcap("MakeText"))\
           .withColumn("MakeText", regexp_replace("MakeText", "Bmw","BMW")) \
           .withColumn("MakeText", regexp_replace("MakeText", "Vw", "VW")) \
           .withColumn("MakeText", regexp_replace("MakeText", "Mercedes-Benz", "Mercedes Benz"))

        # Save to csv file, repartitioning for easy view
        newbody \
        .repartition(1)\
        .write.format("com.databricks.spark.csv")\
        .mode("overwrite")\
        .option("header", "true")\
        .option("charset", "UTF-8")\
        .save(tgt_file,format="csv")
        print(2)
        return 1
    except Exception as e:
        return 0
