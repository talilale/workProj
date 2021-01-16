from utils import *
from pyspark.sql.functions import *
import pyspark.sql.functions as f
import os
import logging
import datetime as dt

now = str(dt.datetime.now().strftime('%Y%m%d%H%M%S'))
dir_path = os.path.dirname(os.path.realpath(__file__))


def run(spark, log_file):
    try:
        logging.getLogger(log_file).setLevel(logging.INFO)
        src_file = dir_path + '/data_folder/phaseOne'
        tgt_file = dir_path + '/data_folder/phaseTwo'
        code_file = dir_path + '/data_folder/code_color.csv'
        logging.info(now + ' Starting phase two, normalisation')

        logging.info(now + ' P2: Reading file: ' + src_file)
        df = spark.read.option("charset", "UTF-8").option("header",True).csv(src_file)
        msg = ' P2: Source file - rows: ' + str(df.count()) + ' columns:' + str(len(df.columns))
        logging.info(now + msg)
        # Created a code file for colors based on distinct value from source file and entered translations based on target table.
        # Could use a translator api or just prepare a dict. In normal environment I'd create a code/mapping files/tables/dict
        # for every column that are element types and there is a difference in naming between source and target.
        logging.info(now + ' P2: Reading code file: ' + code_file)
        dfColor = spark.read.option("charset", "UTF-8").option("header",True).option("delimiter", ";").csv(code_file)
        # Target table doesnt differentiate on mat/metallic so cutting color only
        df = df.withColumn("BodyColorText", substring_index("BodyColorText", " ", 1))
        # Joining main dataframe with color dataframe, removing unnecessary columns, switching translation column name,
        # performing additional operations to initcap car names and fixing some big car brands, also should be mapped.
        logging.info(now + ' P2: Joining source data with color translations, normalizing data')
        full_df = df.join(dfColor, df.BodyColorText == dfColor.german, how = 'left') \
            .drop("BodyColorText", "german") \
            .withColumnRenamed("english", "color") \
            .withColumn("MakeText", f.trim("MakeText")) \
            .withColumn("MakeText", initcap("MakeText")) \
            .withColumn("MakeText", regexp_replace("MakeText", "Bmw","BMW")) \
            .withColumn("MakeText", regexp_replace("MakeText", "Vw", "VW")) \
            .withColumn("MakeText", regexp_replace("MakeText", "Mercedes-Benz", "Mercedes Benz"))

        # Save to csv file, repartitioning for easy view
        msg = ' P2: Target file - rows: ' + str(full_df.count()) + ' columns:' + str(len(full_df.columns))
        logging.info(now + msg)
        save_file(full_df, tgt_file)
        logging.info(now + ' P2: Completed')
        return True
    except Exception as e:
        logging.getLogger(log_file).setLevel(logging.ERROR)
        logging.error(now + str(e))
        return False
