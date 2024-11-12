"""
JOIN
"""

from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.functions import col, weekofyear, year # type: ignore

def run_code(spark):
    print("=================================")
    print("Combine data from multiple tables using JOINs")
    print("=================================")
    spark.sql("USE tpch")
    
    # Read tables
    orders = spark.table("orders")
    lineitem = spark.table("lineitem")
    nation = spark.table("nation")
    region = spark.table("region")

    print("======================================")
    print("INNER JOIN")
    print("======================================")
    inner_join_result = (
        orders.alias("o")
        .join(
            lineitem.alias("l"),
            (col("o.orderdate") == col("l.orderkey"))
            & (
                col("o.orderdate").between(
                    col("l.shipdate") - 5, col("l.shipdate") + 5
                )
            ),
        )
        .select("o.orderkey", "l.orderkey")
        .limit(10)
    )
    inner_join_result.show()

if __name__ == '__main__':
    spark_obj = (
        SparkSession.builder.appName("efficient-data-processing")
        .enableHiveSupport()
        .getOrCreate()
    )
    # Set the log level
    spark_obj.sparkContext.setLogLevel("ERROR")
    run_code(spark=spark_obj)
    spark_obj.stop()