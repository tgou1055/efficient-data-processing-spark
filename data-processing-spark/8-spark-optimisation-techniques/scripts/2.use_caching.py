"""
Reduce Data Shuffle
"""
from pyspark.sql import SparkSession # type: ignore # pylint: disable=import-error
#from pyspark.sql.functions import col, sum # type: ignore # pylint: disable=import-error

def run_code_1(spark):
    """
    Spark code runner

    :param: spark: sparkSession object
    :returns: 
    """
    print("=======================================")
    print("Repartition Only")
    print("=======================================")
    # Read data from Minio
    parkViolations = spark.read.option("header", True) \
                          .csv("s3a://data/parking_data/parking_violations/") # pylint: disable=invalid-name
    # Do repartition by 'Plate Type'
    parkViolationsPlateTypeDF = parkViolations.repartition(89, "Plate Type") # pylint: disable=invalid-name
    # Save the count of Plate Type to csv
    plateTypeCountDF = parkViolationsPlateTypeDF.groupBy("Plate Type").count() # pylint: disable=invalid-name
    plateTypeCountDF.write.format("com.databricks.spark.csv").option("header", True) \
                    .mode("overwrite") \
                    .save("s3a://data/parking_data/plate_type_count.csv")
    # Do average aggregation
    plateTypeAvgDF = parkViolationsPlateTypeDF.groupBy("Plate Type").avg() # pylint: disable=invalid-name
    # avg() is not meaningful here, but used just as an aggregation example
    plateTypeAvgDF.write.format("com.databricks.spark.csv").option("header", True) \
                    .mode("overwrite") \
                    .save("s3a://data/parking_data/plate_type_count.csv")

    # P.S.
    # You will see that we are redoing the repartition step each time for plateTypeCountDF and
    # plateTypeAvgDF dataframe. We can prevent the second repartition by caching the result of
    # the first repartition, as shown below.

def run_code_2(spark):
    """
    Spark code runner

    :param: spark: sparkSession object
    :returns:
    """
    print("=======================================")
    print("Repartition with Caching")
    print("=======================================")
    # Read data from Minio
    parkViolations = spark.read.option("header", True) \
                          .csv("s3a://data/parking_data/parking_violations/") # pylint: disable=invalid-name
    # Do repartition by 'Plate Type'
    parkViolationsPlateTypeDF = parkViolations.repartition(89, "Plate Type") # pylint: disable=invalid-name
    # Cache the required field of the DataFrame to keep cache size small
    cachedDF = parkViolationsPlateTypeDF.select('Plate Type').cache() # pylint: disable=invalid-name
    # Save the count of Plate Type to csv
    plateTypeCountDF = cachedDF.groupBy("Plate Type").count() # pylint: disable=invalid-name
    plateTypeCountDF.write.format("com.databricks.spark.csv").option("header", True) \
                    .mode("overwrite") \
                    .save("s3a://data/parking_data/plate_type_count.csv")
    # Do average aggregation
    plateTypeAvgDF = cachedDF.groupBy("Plate Type").avg() # pylint: disable=invalid-name
    # avg() is not meaningful here, but used just as an aggregation example
    plateTypeAvgDF.write \
        .format("com.databricks.spark.csv").option("header", True) \
        .mode("overwrite") \
        .save("s3a://data/parking_data/plate_type_count.csv")

    # P.S.
    # Here you will see that the construction of plateTypeAvgDF dataframe does not involve 
    # the file scan and repartition, because that dataframe parkViolationsPlateTypeDF 
    # is already in the cluster memory.
if __name__ == '__main__':
    spark_obj = (
        SparkSession.builder.appName("efficient-data-processing-spark")
        .enableHiveSupport()
        .getOrCreate()
    )
    # Set the log level
    spark_obj.sparkContext.setLogLevel("ERROR")
    spark_obj.conf.set("spark.sql.adaptive.enabled", "false")
    #run_code_1(spark=spark_obj)
    run_code_2(spark=spark_obj)
    spark_obj.stop()
