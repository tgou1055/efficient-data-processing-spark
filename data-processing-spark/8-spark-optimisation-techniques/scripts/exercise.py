"""
Exercise

Consider that you have to save the parkViolations into parkViolationsNY, 
parkViolationsNJ, parkViolationsCT, parkViolationsAZ depending on the Registration State field. 

Will caching help here, if so how?

Key points

    1. If you are using a particular data frame multiple times, try caching the dataframe’s 
       necessary columns to prevent multiple reads from disk and reduce the size of dataframe 
       to be cached.
    2. One thing to be aware of is the cache size of your cluster, 
       do not cache data frames if not necessary.
    3. The tradeoff in terms of speed is the time taken to cache your dataframe in memory.
    4. If you need a way to cache a data frame part in memory and part in disk or other 
       such variations refer to 'persist'.

"""
from pyspark.sql import SparkSession # type: ignore # pylint: disable=import-error
from pyspark.sql.functions import col # type: ignore # pylint: disable=import-error

def run_code(spark):
    """
    Spark code runner

    :param: spark: sparkSession object
    :returns: 
    """
    print("=======================================")
    print("Exercise")
    print("=======================================")
    # Read data from Minio
    park_violations = spark.read.option("header", True) \
                          .csv("s3a://data/parking_data/parking_violations/")
    # Explore how many states there are.
    #park_violations_by_state = park_violations.groupBy('Registration State')
    #print(park_violations_by_state.count().count())
    # The number of 'Registration State' is 69

    # Do Repartition base on number of 'Registration State'
    park_violations_by_state = park_violations.repartition(69, 'Registration State')
    # Cache the required field of the DataFrame to keep cache size small
    # Count the number of parking violations for each Registration State

    park_violations_ny = park_violations_by_state.filter(col("Registration State") == "NY").cache()
    park_violations_nj = park_violations_by_state.filter(col("Registration State") == "NJ").cache()
    park_violations_ct = park_violations_by_state.filter(col("Registration State") == "CT").cache()
    park_violations_az = park_violations_by_state.filter(col("Registration State") == "AZ").cache()

    print(park_violations_ny.count())
    print(park_violations_nj.count())
    print(park_violations_ct.count())
    print(park_violations_az.count())

    # Peform aggregations and save queries for each state using caches df.

if __name__ == '__main__':
    spark_obj = (
        SparkSession.builder.appName("efficient-data-processing-spark")
        .getOrCreate()
    )
    # Set the log level
    spark_obj.sparkContext.setLogLevel("ERROR")
    spark_obj.conf.set("spark.sql.adaptive.enabled", "false")
    run_code(spark=spark_obj)
    spark_obj.stop()
