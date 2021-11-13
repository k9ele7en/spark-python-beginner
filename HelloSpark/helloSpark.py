from pyspark import SparkConf
from pyspark.sql import *

from lib.logger import Log4J
from lib.utils import get_spark_app_config

if __name__ == "__main__":
    # m1-directly
    # spark = SparkSession.builder \
    #     .appName("hello spark") \
    #     .master("local[3]") \
    #     .getOrCreate()

    # m2-set by SparkConf
    # conf = SparkConf()
    # conf.set("spark.app.name", "Hello Spark")
    # conf.set("spark.master", "local[3]")
    # spark = SparkSession.builder \
    #     .config(conf=conf) \
    #     .getOrCreate()

    # m3 from conf file
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4J(spark)

    logger.info("Sparting driver")

    conf_out = spark.sparkContext.getConf()
    logger.info(conf_out.toDebugString())

    spark.stop()
    logger.info("Finished driver")
