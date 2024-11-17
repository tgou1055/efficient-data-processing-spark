
"""
Bucket Join
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
    print("Join large dataset with bucketing ")
    print("=======================================")

    # Read data from CSV files
    park_violations_2015 = spark.read \
                                .option("header", True) \
                                .csv("s3a://data/parking_data/parking_violations/2015.csv")
    park_violations_2016 = spark.read \
                                .option("header", True) \
                                .csv("s3a://data/parking_data/parking_violations/2016.csv")
    
    # Modify the column names
    new_column_name_list = list(map(lambda x: x.replace(" ", "_"), park_violations_2015.columns))

    print(new_column_name_list)
    # Replace dfs with new column names and filer out "COM" and "2001" to limit join process time
    park_violations_2015 = park_violations_2015.toDF(*new_column_name_list)
    park_violations_2015 = park_violations_2015 \
                                        .filter(park_violations_2015["Plate_Type"] == "COM") \
                                        .filter(park_violations_2015["Vehicle_Year"] == "2001")
    park_violations_2016 = park_violations_2016.toDF(*new_column_name_list)
    park_violations_2016 = park_violations_2016 \
                                        .filter(park_violations_2016["Plate_Type"] == "COM") \
                                        .filter(park_violations_2016["Vehicle_Year"] == "2001")

    # Disable auto broadcastjoin (-1: diable)
    spark.conf.set("spark.sql.autoBroadcastJoinThreadhold", -1)

    # Bucketing and saving as tables
    park_violations_2015.write \
                        .mode("overwrite") \
                        .bucketBy(400, "Vehicle_Year", "Plate_Type") \
                        .saveAsTable("park_violations_2015_bucket")

    park_violations_2016.write \
                        .mode("overwrite") \
                        .bucketBy(400, "Vehicle_Year", "Plate_Type") \
                        .saveAsTable("park_violations_2016_bucket")
    
    # Read tables
    park_violations_2015_table = spark.read \
                                      .table("park_violations_2015_bucket")
    park_violations_2016_table = spark.read \
                                      .table("park_violations_2016_bucket")
    # Join tables
    join_df = park_violations_2015_table.join(
        park_violations_2016_table,
        (park_violations_2015_table["Plate_Type"] ==  park_violations_2016_table["Plate_Type"])
        & (park_violations_2015_table["Vehicle_Year"] ==  park_violations_2016_table["Vehicle_Year"]), 
        how="inner"
    ).select(park_violations_2015_table["Summons_Number"], park_violations_2016_table["Issue_Date"])

    join_df.explain() # Only SortMergeJoin, no exchange, which means no data shuffle

    # Save to CSV
    join_df.write \
            .format("com.databricks.spark.csv") \
            .option("header", True) \
            .mode("overwrite") \
            .save("s3a://data/parking_data/joined_df_bucket.csv")
    
if __name__ == '__main__':
    spark_obj = (
        SparkSession.builder.appName("efficient-data-processing-spark")
        .getOrCreate()
    )
    # Set the log level
    spark_obj.sparkContext.setLogLevel("ERROR")
    spark_obj.conf.set("spark.sql.adaptive.enabled", "false")
    run_code_1(spark=spark_obj)
    spark_obj.stop()
