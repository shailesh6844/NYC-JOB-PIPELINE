from pyspark.sql.functions import col, split, explode, lower


# ---------------------------------------------------------
# KPI 1: Top Job Categories by Number of Postings
# ---------------------------------------------------------
def kpi_top_job_categories(df):
    """
    Calculates the total number of job postings per job category
    and ranks them in descending order.

    Returns:
        DataFrame: Job Category with Total_Postings sorted highest to lowest.
    """
    return df.groupBy("Job Category") \
        .count() \
        .withColumnRenamed("count", "Total_Postings") \
        .orderBy("Total_Postings", ascending=False)


# ---------------------------------------------------------
# KPI 2: Average Salary by Job Category
# ---------------------------------------------------------
def kpi_salary_distribution(df):
    """
    Computes the average salary for each job category.

    Returns:
        DataFrame: Job Category with Avg_Salary_By_Category.
    """
    return df.groupBy("Job Category") \
        .avg("Avg_Salary") \
        .withColumnRenamed("avg(Avg_Salary)", "Avg_Salary_By_Category")


# ---------------------------------------------------------
# KPI 3: Highest Salary Offered by Each Agency
# ---------------------------------------------------------
def kpi_highest_salary_agency(df):
    """
    Identifies the maximum average salary offered by each agency.

    Returns:
        DataFrame: Agency with Highest_Salary.
    """
    return df.groupBy("Agency") \
        .max("Avg_Salary") \
        .withColumnRenamed("max(Avg_Salary)", "Highest_Salary")


# ---------------------------------------------------------
# KPI 4: Average Salary by Agency (Last 2 Years)
# ---------------------------------------------------------
def kpi_avg_salary_last_2years(df):
    """
    Calculates the average salary per agency for the most recent
    two posting years available in the dataset.

    Steps:
    1. Identify the latest posting year in the dataset.
    2. Filter records from the latest year and the previous year.
    3. Compute average salary per agency.

    Returns:
        DataFrame: Agency with Avg_Salary_Last_2Years.
    """
    # Get the latest year from the dataset
    latest_year = df.selectExpr("max(Posting_Year)").collect()[0][0]

    # Filter data for the latest two years
    df_last2 = df.filter(col("Posting_Year") >= latest_year - 1)

    return df_last2.groupBy("Agency") \
        .avg("Avg_Salary") \
        .withColumnRenamed("avg(Avg_Salary)", "Avg_Salary_Last_2Years")


# ---------------------------------------------------------
# KPI 5: Average Salary by Degree Level
# ---------------------------------------------------------
def kpi_degree_salary(df):
    """
    Computes the average salary based on required degree level.

    Returns:
        DataFrame: Degree_Level with Avg_Salary.
    """
    return df.groupBy("Degree_Level") \
        .avg("Avg_Salary") \
        .withColumnRenamed("avg(Avg_Salary)", "Avg_Salary")


# ---------------------------------------------------------
# KPI 6: Highest Paying Skills
# ---------------------------------------------------------
def kpi_highest_paid_skills(df):
    """
    Identifies the highest-paying skills based on average salary.

    Steps:
    1. Split the 'Preferred Skills' column into individual skills.
    2. Convert skills to lowercase for consistency.
    3. Explode the array into separate rows.
    4. Calculate average salary per skill.
    5. Sort skills by highest average salary.

    Returns:
        DataFrame: skill with Avg_Salary sorted descending.
    """
    # Split comma-separated skills into rows
    skill_df = df.withColumn(
        "skill",
        explode(split(lower(col("Preferred Skills")), ","))
    )

    return skill_df.groupBy("skill") \
        .avg("Avg_Salary") \
        .withColumnRenamed("avg(Avg_Salary)", "Avg_Salary") \
        .orderBy("Avg_Salary", ascending=False)
