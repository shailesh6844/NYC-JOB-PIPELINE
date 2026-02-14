from pyspark.sql.functions import col, when, year, current_date, datediff

def feature_engineering(df):
    """
    Performs feature engineering on the job dataset by creating
    additional derived columns for salary insights, job classification,
    education requirements, and posting timeline.

    Parameters:
        df (DataFrame): Input Spark DataFrame containing job data.

    Returns:
        DataFrame: Transformed DataFrame with new engineered features.
    """

    # Create average salary column
    # Calculates the midpoint between minimum and maximum salary
    df = df.withColumn(
        "Avg_Salary",
        (col("Salary Range From") + col("Salary Range To")) / 2
    )

    # Create salary spread column
    # Represents the difference between maximum and minimum salary
    df = df.withColumn(
        "Salary_Spread",
        col("Salary Range To") - col("Salary Range From")
    )

    # Categorize salary into bands (Low, Medium, High)
    # Based on average salary thresholds
    df = df.withColumn(
        "Salary_Band",
        when(col("Avg_Salary") < 60000, "Low")
        .when(col("Avg_Salary").between(60000, 100000), "Medium")
        .otherwise("High")
    )

    # Determine seniority level based on keywords in Business Title
    # Uses case-insensitive regex matching
    df = df.withColumn(
        "Seniority_Level",
        when(col("Business Title").rlike("(?i)senior|lead|chief"), "Senior")
        .when(col("Business Title").rlike("(?i)manager|director"), "Management")
        .otherwise("Junior")
    )

    # Extract degree requirement level from Minimum Qualification text
    # Identifies PhD, Masters, Bachelor, or assigns "Other"
    df = df.withColumn(
        "Degree_Level",
        when(col("Minimum Qual Requirements").rlike("(?i)phd"), "PhD")
        .when(col("Minimum Qual Requirements").rlike("(?i)master"), "Masters")
        .when(col("Minimum Qual Requirements").rlike("(?i)bachelor"), "Bachelor")
        .otherwise("Other")
    )

    # Extract the year from the posting date
    df = df.withColumn("Posting_Year", year(col("Posting Date")))

    # Calculate how many days have passed since the job was posted
    df = df.withColumn(
        "Job_Posting_Age",
        datediff(current_date(), col("Posting Date"))
    )

    # Return the DataFrame with engineered features
    return df
