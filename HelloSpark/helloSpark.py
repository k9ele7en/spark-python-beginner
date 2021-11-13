from pyspark.sql import *

from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("hello spark") \
        .master("local[3]") \
        .getOrCreate()
    print("Starting Hello spark")

    logger = Log4J(spark)

    logger.info("Sparting driver")
    spark.stop()
    logger.info("Finished driver")
