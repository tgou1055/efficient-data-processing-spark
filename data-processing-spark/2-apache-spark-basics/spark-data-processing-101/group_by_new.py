"""
GROUP BY 
"""

from pyspark.sql import SparkSession # type: ignore 
from pyspark.sql.functions import count # type: ignore


def run_code(spark):
    """
    Spark code runner

    :param spark: sparkSession object

    :return
    """
    print("=================================")
    print("Generate metrics for your dimension(s) using GROUP BY")
    print("=================================")
    # Read the 'orders' table
    spark.sql("USE tpch")
    orders = spark.table("orders")

    print("=================================")
    print("Perform aggregation using DataFrame API")
    print("=================================")
    result = (
        orders.groupby("orderpriority")
        .agg(count("*").alias("num_orders"))
        .orderBy("orderpriority")
    )

    # Show the result
    result.show()

if __name__ == '__main__':
    spark_obj = (
        SparkSession.builder.appName("efficient-data-processing-spark")
        .enableHiveSupport()
        .getOrCreate()
    )
    # Set the log level
    spark_obj.sparkContext.setLogLevel("ERROR")
    run_code(spark=spark_obj)
    spark_obj.stop()
