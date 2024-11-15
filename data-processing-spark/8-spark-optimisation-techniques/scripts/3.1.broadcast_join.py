"""
Broadcast Join
"""
from pyspark.sql import SparkSession # type: ignore # pylint: disable=import-error
from pyspark.sql.functions import col # type: ignore # pylint: disable=import-error

def run_code_1(spark):
    """
    Spark code runner

    :param: spark: sparkSession object
    :return
    """
    print("=======================================")
    print("Join without Broadcast")
    print("=======================================")
    # Read CSV files from Minio
    park_violations_2015 = spark.read \
                                .option("header", True) \
                                .csv("s3a://data/parking_data/parking_violations/2015.csv")
    park_violations_2016 = spark.read \
                                .option("header", True) \
                                .csv("s3a://data/parking_data/parking_violations/2016.csv")

    # Rename the column for easier joins
    park_violations_2015 = park_violations_2015.withColumnRenamed("Plate Type", "plateType")
    park_violations_2016 = park_violations_2016.withColumnRenamed("Plate Type", "plateType")

    # Filter out the plate type of "COM"
    park_violations_2015_com = park_violations_2015.filter(col("plateType") == "COM" )
    park_violations_2016_com = park_violations_2016.filter(col("plateType") == "COM" )

    # Inner Join
    join_df = park_violations_2015_com.join(park_violations_2016_com, on="plateType", how="inner") \
                                      .select(park_violations_2015_com["Summons Number"],
                                              park_violations_2016_com["Issue Date"]
                                        )
    join_df.explain()

    # Save to csv (will take a long time to run)
    join_df.write \
        .format("com.databricks.spark.csv") \
        .option("header", True) \
        .mode("overwrite") \
        .save("s3a://data/parking_data/join_df")


def run_code_2(spark):
    """
    Spark code runner

    :param: spark: sparkSession object
    :return
    """
    print("=======================================")
    print("Join with Broadcast")
    print("=======================================")
    # Read CSV files from Minio
    park_violations_2015 = spark.read \
                                .option("header", True) \
                                .csv("s3a://data/parking_data/parking_violations/2015.csv")
    park_violations_2016 = spark.read \
                                .option("header", True) \
                                .csv("s3a://data/parking_data/parking_violations/2016.csv")

    # Rename the column for easier joins
    park_violations_2015 = park_violations_2015.withColumnRenamed("Plate Type", "plateType")
    park_violations_2016 = park_violations_2016.withColumnRenamed("Plate Type", "plateType")

    # Filter out the plate type of "COM"
    park_violations_2015_com = park_violations_2015.filter(col("plateType") == "COM" ) \
                                                .select("plateType", "Summons Number") \
                                                .distinct()
    park_violations_2016_com = park_violations_2016.filter(col("plateType") == "COM" ) \
                                                .select("plateType", "Issue Date") \
                                                .distinct()
    # Cache them into memory
    park_violations_2015_com.cache()
    park_violations_2016_com.cache()

    park_violations_2015_com.count() # This will cause df to be cached
    park_violations_2015_com.count() # This will cause df to be cached

    # Join the data frames with as broadcast join
    join_df = park_violations_2015_com.join(park_violations_2016_com.hint("broadcast"),
                                            on="plateType", how="inner") \
                                      .select(park_violations_2015_com["Summons Number"],
                                              park_violations_2016_com["Issue Date"]
                                        )
    join_df.explain()

    # Save to csv (will take a long time to run)
    join_df.write \
        .format("com.databricks.spark.csv") \
        .option("header", True) \
        .mode("overwrite") \
        .save("s3a://data/parking_data/join_df")

if __name__ == '__main__':
    spark_obj = (
        SparkSession.builder.appName("efficient-data-processing-spark")
        .getOrCreate()
    )
    # Set the log level
    spark_obj.sparkContext.setLogLevel("ERROR")
    spark_obj.conf.set("spark.sql.adaptive.enabled", "false")
    run_code_2(spark=spark_obj)
    spark_obj.stop()
