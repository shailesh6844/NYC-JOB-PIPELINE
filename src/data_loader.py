def load_data(spark, path):
    """
    Loads a CSV file into a Spark DataFrame.

    Parameters:
        spark (SparkSession): Active Spark session used to read the data.
        path (str): File path to the CSV dataset.

    Returns:
        DataFrame: Spark DataFrame containing the loaded data.
    """

    # Read the CSV file with the following options:
    # - header=True: Treat the first row as column headers
    # - inferSchema=True: Automatically detect column data types
    # - multiLine=True: Allow fields to span multiple lines
    # - escape='"': Handle quoted values properly
    df = spark.read.csv(
        path,
        header=True,
        inferSchema=True,
        multiLine=True,
        escape='"'
    )

    # Return the loaded DataFrame
    return df
