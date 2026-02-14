"""
NYC Jobs Data Pipeline - Apache Airflow DAG

This DAG orchestrates an end-to-end Spark-based data pipeline for the
NYC Jobs dataset. The pipeline performs:

1. Spark Session Initialization
2. Data Loading
3. Data Cleaning
4. Feature Engineering
5. KPI Computation
6. Visualization Generation
7. Processed Data Storage
8. Resource Cleanup

The DAG runs daily and executes tasks sequentially.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os


# ---------------------------------------------------------
# Project Path Configuration
# ---------------------------------------------------------
# Dynamically adds the project root directory to the Python path
# so internal modules under /src can be imported correctly.
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)


# ---------------------------------------------------------
# Import Project Modules
# ---------------------------------------------------------
from src.spark_session import create_spark_session
from src.data_loader import load_data
from src.data_cleaning import clean_data
from src.feature_engineering import feature_engineering
from src.kpi_builder import (
    kpi_top_job_categories,
    kpi_salary_distribution,
    kpi_highest_salary_agency,
    kpi_avg_salary_last_2years,
    kpi_degree_salary,
    kpi_highest_paid_skills
)
from src.utils import save_output
from src.visualization import matplotlib_bar


# ---------------------------------------------------------
# Global Variables (Used for Task Data Sharing)
# ---------------------------------------------------------
# NOTE:
# In production-grade Airflow, XCom or external storage should be used
# instead of global variables. These are used here for simplicity.
spark = None
df_raw = None
df_cleaned = None
df_featured = None
kpis = {}


# ---------------------------------------------------------
# Task 1: Initialize Spark Session
# ---------------------------------------------------------
def task_initialize_spark(**context):
    """
    Initializes a Spark session required for distributed data processing.
    """
    global spark
    print("Initializing Spark session...")
    spark = create_spark_session()
    print("Spark session initialized successfully")


# ---------------------------------------------------------
# Task 2: Load Raw Data
# ---------------------------------------------------------
def task_load_data(**context):
    """
    Loads the raw NYC jobs dataset from CSV into a Spark DataFrame.
    Pushes row count to XCom for monitoring.
    """
    global spark, df_raw
    print("Loading data from CSV...")
    df_raw = load_data(spark, "/opt/airflow/dataset/nyc-jobs.csv")

    row_count = df_raw.count()
    print(f"Data loaded successfully. Row count: {row_count}")

    context['ti'].xcom_push(key='row_count_raw', value=row_count)


# ---------------------------------------------------------
# Task 3: Data Cleaning
# ---------------------------------------------------------
def task_clean_data(**context):
    """
    Cleans raw dataset by handling nulls, formatting columns,
    and standardizing data.
    """
    global df_raw, df_cleaned
    print("Cleaning data...")
    df_cleaned = clean_data(df_raw)

    row_count = df_cleaned.count()
    print(f"Data cleaned successfully. Row count: {row_count}")

    context['ti'].xcom_push(key='row_count_cleaned', value=row_count)


# ---------------------------------------------------------
# Task 4: Save Cleaned Data
# ---------------------------------------------------------
def task_save_cleaned_data(**context):
    """
    Saves cleaned dataset in CSV format for downstream validation
    or external usage.
    """
    global df_cleaned
    print("Saving cleaned data (CSV)...")
    save_output(df_cleaned, "output/cleaned_jobs_csv", format="csv")
    print("Cleaned data saved successfully")


# ---------------------------------------------------------
# Task 5: Feature Engineering
# ---------------------------------------------------------
def task_feature_engineering(**context):
    """
    Applies feature engineering transformations such as:
    - Salary aggregation
    - Year extraction
    - Skill parsing
    """
    global df_cleaned, df_featured
    print("Applying feature engineering...")
    df_featured = feature_engineering(df_cleaned)

    row_count = df_featured.count()
    print(f"Feature engineering completed. Row count: {row_count}")

    context['ti'].xcom_push(key='row_count_featured', value=row_count)


# ---------------------------------------------------------
# Task 6: KPI Computation
# ---------------------------------------------------------
def task_build_kpis(**context):
    """
    Computes all business KPIs:
    1. Top Job Categories
    2. Salary Distribution by Category
    3. Highest Salary by Agency
    4. Average Salary (Last 2 Years)
    5. Salary by Degree Level
    6. Highest Paying Skills
    """
    global df_featured, kpis
    print("Building KPIs...")

    kpis['kpi1'] = kpi_top_job_categories(df_featured)
    kpis['kpi2'] = kpi_salary_distribution(df_featured)
    kpis['kpi3'] = kpi_highest_salary_agency(df_featured)
    kpis['kpi4'] = kpi_avg_salary_last_2years(df_featured)
    kpis['kpi5'] = kpi_degree_salary(df_featured)
    kpis['kpi6'] = kpi_highest_paid_skills(df_featured)

    print("All KPIs built successfully")


# ---------------------------------------------------------
# Task 7: Visualization Generation
# ---------------------------------------------------------
def task_generate_visualizations(**context):
    """
    Generates and saves visualizations for all KPIs
    using Matplotlib.
    """
    global kpis
    print("Generating visualizations...")
    viz_output = "output/visualizations"

    matplotlib_bar(kpis['kpi1'], "Job Category", "Total_Postings",
                   "Top Job Categories", viz_output, "kpi1_top_job_categories.png")

    matplotlib_bar(kpis['kpi2'], "Job Category", "Avg_Salary_By_Category",
                   "Salary Distribution by Category", viz_output, "kpi2_salary_distribution.png")

    matplotlib_bar(kpis['kpi3'], "Agency", "Highest_Salary",
                   "Highest Paying Agencies", viz_output, "kpi3_highest_salary_agency.png")

    matplotlib_bar(kpis['kpi4'], "Agency", "Avg_Salary_Last_2Years",
                   "Average Salary (Last 2 Years)", viz_output, "kpi4_avg_salary_recent.png")

    matplotlib_bar(kpis['kpi5'], "Degree_Level", "Avg_Salary",
                   "Average Salary by Degree", viz_output, "kpi5_degree_salary.png")

    matplotlib_bar(kpis['kpi6'], "skill", "Avg_Salary",
                   "Highest Paid Skills", viz_output, "kpi6_highest_paid_skills.png")

    print("All visualizations generated successfully")


# ---------------------------------------------------------
# Task 8: Save Final Processed Data
# ---------------------------------------------------------
def task_save_processed_data(**context):
    """
    Saves the fully processed and feature-engineered dataset
    in Parquet format for analytics consumption.
    """
    global df_featured
    print("Saving processed data...")
    save_output(df_featured, "output/processed_jobs")
    print("Processed data saved successfully")


# ---------------------------------------------------------
# Task 9: Cleanup
# ---------------------------------------------------------
def task_cleanup(**context):
    """
    Stops Spark session to release cluster resources.
    This task runs regardless of upstream success/failure.
    """
    global spark
    print("Cleaning up Spark session...")
    if spark:
        spark.stop()
        print("Spark session stopped")


# ---------------------------------------------------------
# DAG Configuration
# ---------------------------------------------------------
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 2, 14),
}


# ---------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------
dag = DAG(
    'nyc_jobs_pipeline',
    default_args=default_args,
    description='NYC Jobs Data Pipeline - Spark + Airflow',
    schedule_interval='@daily',
    catchup=False,
    tags=['data-pipeline', 'nyc-jobs', 'spark'],
)
