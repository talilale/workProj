from pyspark.sql import SparkSession


def init_spark():
    return SparkSession.builder.appName("pipeline") \
        .getOrCreate()


def save_file(df, file_name):
    df.repartition(1) \
        .write.format("com.databricks.spark.csv") \
        .mode("overwrite") \
        .option("header", "true") \
        .option("charset", "UTF-8") \
        .save(file_name, format="csv")
