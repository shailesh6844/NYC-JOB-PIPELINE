from pyspark.sql.functions import col

def clean_data(df):
    """
    Cleans the input Spark DataFrame by:
    1. Selecting only the required columns.
    2. Removing duplicate records.
    3. Filtering out rows with missing salary range values.
    
    Parameters:
        df (DataFrame): Input Spark DataFrame containing job data.
        
    Returns:
        DataFrame: Cleaned Spark DataFrame.
    """

    # List of columns required for analysis
    required_columns = [
        "Job ID",
        "Agency",
        "Business Title",
        "Job Category",
        "Minimum Qual Requirements",
        "Preferred Skills",
        "Salary Range From",
        "Salary Range To",
        "Posting Date"
    ]

    # Select only the required columns from the dataset
    df = df.select(required_columns)

    # Remove duplicate rows to ensure data uniqueness
    df = df.dropDuplicates()

    # Filter out records where salary range values are missing
    df = df.filter(
        col("Salary Range From").isNotNull() &
        col("Salary Range To").isNotNull()
    )

    # Return the cleaned DataFrame
    return df
