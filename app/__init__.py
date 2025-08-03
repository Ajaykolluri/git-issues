from pyspark.sql import SparkSession

from app.config import AppConfig


app_cfg = AppConfig()

spark = SparkSession.builder \
    .appName("MyPySparkApp") \
    .getOrCreate()
spark.sparkContext.setLogLevel("INFO")

