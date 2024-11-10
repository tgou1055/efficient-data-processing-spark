"""
1. Data type (wrong type) handling
2. Coalesce to default value in case of Null
"""

from pyspark.sql import SparkSession # type: ignore # pylint: disable=import-error
from pyspark.sql.functions import coalesce, col, datediff, lit # type: ignore # pylint: disable=import-error

def run_code(spark_obj):
    """
    Spark code runner

    params:
        :param spark: sparkSession object

    returns:

    """
    print("=================================")
    print("The following is supposed to fail, but spark \
           makes it work and generate a NULL! \
           (Not what we expect)"
    )
    df_invalid_date_diff = spark_obj.createDataFrame(
        [("2024-11-10", "invalid_date")], ["date1", "date2"]
    )
    df_result_1 = df_invalid_date_diff.withColumn(
        "diff", datediff("date1", "date2")
    )
    df_result_1.show()

    # Alternative way
    df_result_2 = (
        df_invalid_date_diff.withColumn(
            "date1", df_invalid_date_diff["date1"].cast("date")
        )
        .withColumn("date2", df_invalid_date_diff["date2"].cast("date"))
        .withColumn("diff", datediff("date1", "date2"))
    )
    df_result_2.show()


    print("=================================")
    print("Coalesce to default value in case of Null")
    print("=================================")
    orders = spark_obj.table("tpch.orders")
    lineitem = spark_obj.table("tpch.lineitem")

    df_join = (
        orders.alias("o")
        .join(
            lineitem.alias("l"),
            (col("o.orderkey") == col("l.orderkey"))
            & (
                col("o.orderdate").between(
                    col("l.shipdate") - 5, col("l.shipdate") + 5
                )
            ),
            "left",
        )
        .select(
            col("o.orderkey"),
            col("o.orderdate"),
            coalesce(col("l.orderkey"), lit(9999999)).alias(
                "lineitem_orderkey"
            ),
            col("l.shipdate"),
        )
        .filter(col("l.shipdate").isNull())
        .limit(20)
    )

    # Display the query result
    df_join.show()

if __name__ == '__main__':
    spark = (
        SparkSession.builder.appName("efficient-spark-data-processing")
        .enableHiveSupport()
        .getOrCreate()
    )
    # enable the hive support to query the table with meta data stored in metadata_db

    # Set the log level
    spark.sparkContext.setLogLevel("ERROR")
    run_code(spark_obj=spark)
    spark.stop()
