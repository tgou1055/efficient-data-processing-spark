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
    print("Data Shuffle Example")
    print("=======================================")
    # Read data from Minio
    parkViolations = spark.read.option("header", True) \
                          .csv("s3a://data/parking_data/parking_violations/") # pylint: disable=invalid-name
    plateTypeCountDF = parkViolations.groupBy("Plate Type").count() # pylint: disable=invalid-name
    plateTypeCountDF.write.format("com.databricks.spark.csv").option("header", True) \
                    .mode("overwrite") \
                    .save("s3a://data/parking_data/plate_type_count")
    plateTypeCountDF.explain() # show the plan before execution
    num_rows = plateTypeCountDF.count()
    plateTypeCountDF.show()
    print(f"Total number of Plate Type: {num_rows}")

def run_code_2(spark):
    """
    Spark code runner

    :param: spark: sparkSession object
    :returns:
    """
    print("=======================================")
    print("Reduce Data Shuffle by Repartition")
    print("=======================================")
    # Read data from Minio
    parkViolations = spark.read.option("header", True) \
                          .csv("s3a://data/parking_data/parking_violations/") # pylint: disable=invalid-name

    # Do repartition by 'Plate Type'
    parkViolationsPlateTypeDF = parkViolations.repartition(89, "Plate Type") # pylint: disable=invalid-name
    parkViolationsPlateTypeDF.explain()
    # you will see a filescan to read data and exchange hashpartition
    # to shuffle and partition based on Plate Type

    plateTypeCountDF = parkViolationsPlateTypeDF.groupBy("Plate Type").count() # pylint: disable=invalid-name
    plateTypeCountDF.write.format("com.databricks.spark.csv").option("header", True) \
                    .mode("overwrite") \
                    .save("s3a://data/parking_data/plate_type_count")
    plateTypeCountDF.explain() # show the plan before execution
    num_rows = plateTypeCountDF.count()
    plateTypeCountDF.show()
    print(f"Total number of Plate Type: {num_rows}")

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
    run_code_2(spark=spark_obj) # compare with run_code_1
    spark_obj.stop()
