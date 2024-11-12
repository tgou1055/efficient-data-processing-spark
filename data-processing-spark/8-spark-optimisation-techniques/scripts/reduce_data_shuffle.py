from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum


def run_code(spark):
    print("=======================================")
    print("Reduce Data Shuffle")
    print("=======================================")
    # Read data from Minio
    parkViolations = spark.read.option("header", True).csv("s3a://parking/parking_violations/")
    parkViolations.show(100)


if __name__ == '__main__':
    spark = (
        SparkSession.builder.appName("efficient-data-processing-spark")
        .enableHiveSupport()
        .getOrCreate()
    )
    # Set the log level
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    run_code(spark=spark)
    spark.stop()