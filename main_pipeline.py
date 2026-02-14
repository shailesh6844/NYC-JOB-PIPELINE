from src.spark_session import create_spark_session
from src.data_loader import load_data
from src.data_cleaning import clean_data
from src.feature_engineering import feature_engineering
from src.kpi_builder import *
from src.utils import save_output
from src.visualization import matplotlib_bar


def run_pipeline():

    spark = create_spark_session()

    df = load_data(spark, "dataset/nyc-jobs.csv")

    df = clean_data(df)

    # Save Cleaned Data
    print("Saving cleaned data (CSV)...")
    save_output(df, "output/cleaned_jobs_csv", format="csv")

    df = feature_engineering(df)

    # Build KPIs
    kpi1 = kpi_top_job_categories(df)
    kpi2 = kpi_salary_distribution(df)
    kpi3 = kpi_highest_salary_agency(df)
    kpi4 = kpi_avg_salary_last_2years(df)
    kpi5 = kpi_degree_salary(df)
    kpi6 = kpi_highest_paid_skills(df)

    # Save Processed Data
    save_output(df, "output/processed_jobs")

    # Generate and Save Visualizations
    print("Generating visualizations...")
    viz_output = "output/visualizations"
    
    # KPI 1
    matplotlib_bar(kpi1, "Job Category", "Total_Postings", "Top Job Categories", viz_output, "kpi1_top_job_categories.png")
    
    # KPI 2
    matplotlib_bar(kpi2, "Job Category", "Avg_Salary_By_Category", "Salary Distribution by Category", viz_output, "kpi2_salary_distribution.png")
    
    # KPI 3 - Highest Salary Agency (might be too many, let's limit in the viz function logic I added or trust the limit)
    matplotlib_bar(kpi3, "Agency", "Highest_Salary", "Highest Paying Agencies", viz_output, "kpi3_highest_salary_agency.png")
    
    # KPI 4
    matplotlib_bar(kpi4, "Agency", "Avg_Salary_Last_2Years", "Average Salary (Last 2 Years)", viz_output, "kpi4_avg_salary_recent.png")
    
    # KPI 5
    matplotlib_bar(kpi5, "Degree_Level", "Avg_Salary", "Average Salary by Degree", viz_output, "kpi5_degree_salary.png")
    
    # KPI 6
    matplotlib_bar(kpi6, "skill", "Avg_Salary", "Highest Paid Skills", viz_output, "kpi6_highest_paid_skills.png")

    return kpi1, kpi2, kpi3, kpi4, kpi5, kpi6


if __name__ == "__main__":
    print("Starting pipeline...")
    kpi1, kpi2, kpi3, kpi4, kpi5, kpi6 = run_pipeline()
    
    print("\nKPI 1: Top Job Categories")
    kpi1.show()
    
    print("\nKPI 2: Salary Distribution")
    kpi2.show()
    
    print("\nKPI 3: Highest Paying Agency")
    kpi3.show()
    
    print("\nKPI 4: Average Salary Last 2 Years")
    kpi4.show()
    
    print("\nKPI 5: Degree vs Salary")
    kpi5.show()
    
    print("\nKPI 6: Highest Paid Skills")
    kpi6.show()

    print("\nPipeline completed successfully!")
