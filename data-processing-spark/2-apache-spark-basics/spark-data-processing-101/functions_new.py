"""
SQL In-Build Functions
"""

from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.functions import ( # type: ignore
    length, concat, split, substring, trim,
    datediff, months_between, date_add, date_format, to_date, to_timestamp,
    year, ceil, floor, round, abs,
    col, lit
) 

def run_code(spark):
    """
    Spark code runner

    params:
        :param: spark: sparkSession object

    returns:

    """
    print("=======================================")
    print("Using tpch database")
    print("=======================================")
    spark.sql("USE tpch")

    print("=======================================")
    print("Calculating length of 'hi'")
    print("=======================================")
    length_df = spark.createDataFrame([('hi',)], ['string'])
    length_df = length_df.select(length(length_df['string']).alias('length'))
    length_df.show()

    print("=======================================")
    print("Concatenating clerk and orderpriority with '-'")
    print("=======================================")
    concat_df = (
        spark.table("orders")
        .select(
            concat(col("clerk"), lit("-"), col("orderpriority"))
            .alias("concatenated")
        )
        .limit(10)
    )
    concat_df.show()

    print("=======================================")
    print("Splitting clerk column by '#' delimiter")
    print("=======================================")
    split_df = (
        spark.table("orders")
        .select(
            split(col("clerk"), "#")
            .alias("split_clerk")
        )
        .limit(10)
    )
    split_df.show()

    print("=======================================")
    print("Extracting first 5 characters of clerk column")
    print("=======================================")
    substr_df = (
        spark.table("orders")
        .select(
            col("clerk"),
            substring("clerk",1,5)
            .alias("clerk_substring")
        )
        .limit(10)
    )
    substr_df.show()


if __name__ == '__main__':
    spark_obj = (
        SparkSession.builder.appName("efficient-data-processing-spark")
        .enableHiveSupport()
        .getOrCreate()
    )
    # Set the log level
    spark_obj.sparkContext.setLogLevel("ERROR")
    run_code(spark=spark_obj)
    spark_obj.stop
