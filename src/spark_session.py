from pyspark.sql import SparkSession


# ---------------------------------------------------------
# Spark Session Initialization
# ---------------------------------------------------------
def create_spark_session():
    """
    Creates and returns a SparkSession for the NYC Jobs Assessment project.

    A SparkSession is the entry point to working with PySpark. It allows
    you to create DataFrames, execute SQL queries, and interact with
    distributed data processing features in Apache Spark.

    Configuration:
    - App Name: "NYC_Jobs_Assessment"

    Returns:
        SparkSession: An active Spark session object.
    """

    # Build and initialize Spark session
    spark = SparkSession.builder \
        .appName("NYC_Jobs_Assessment") \
        .getOrCreate()

    return spark
