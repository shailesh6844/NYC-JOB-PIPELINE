# ---------------------------------------------------------
# Output Writer Utility Function
# ---------------------------------------------------------
def save_output(df, path, format="parquet"):
    """
    Saves a Spark DataFrame to the specified path in the given format.

    Supported Formats:
    - "parquet" (default): Columnar storage format optimized for Spark.
    - "csv": Comma-separated values format with header included.

    Parameters:
        df (DataFrame): The Spark DataFrame to be saved.
        path (str): Destination file path where the data will be written.
        format (str, optional): Output file format ("parquet" or "csv").
                                Default is "parquet".

    Behavior:
    - Uses overwrite mode to replace existing data at the target location.
    - Automatically includes header when saving as CSV.
    - Defaults to Parquet format if any other format is provided.

    Returns:
        None
    """

    # Save as CSV format (with header)
    if format.lower() == "csv":
        df.write.mode("overwrite").option("header", "true").csv(path)

    # Save as Parquet format (default)
    else:
        df.write.mode("overwrite").parquet(path)
