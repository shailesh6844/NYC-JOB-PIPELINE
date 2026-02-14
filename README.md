## Project Overview

This project is an end-to-end Data Engineering pipeline built using PySpark to analyze NYC job postings data and generate actionable salary intelligence insights.

The objective was to transform raw job postings data into structured, analysis-ready datasets and derive business KPIs such as salary trends, highest paid skills, agency salary distribution, and qualification-based salary comparisons.

## Architecture Overview

Raw CSV Data
      ↓
Data Ingestion (PySpark)
      ↓
Data Cleaning
      ↓
Feature Engineering
      ↓
KPI Computation
      ↓
Visualization
      ↓
Processed Output Storage

## Tech Stack

Python 3.x

PySpark

Pandas

Matplotlib

Plotly

Java 11

Spark Standalone (local mode)

## Dataset Description

The dataset contains NYC job postings including:

Job ID

Agency

Business Title

Job Category

Salary Range From / To

Preferred Skills

Minimum Qualification Requirements

Posting Date

## Project Structure

```
nyc_jobs_pipeline/
│
├── data/
├── src/
│   ├── spark_session.py
│   ├── data_loader.py
│   ├── data_cleaning.py
│   ├── feature_engineering.py
│   ├── kpi_builder.py
│   ├── visualization.py
│   └── utils.py
│
├── tests/
├── main_pipeline.py
└── README.md
```


## Pipeline Steps
1️⃣ Data Ingestion

Loaded CSV into Spark DataFrame

Schema validation

2️⃣ Data Cleaning

Removed duplicates

Filtered null salary records

Selected required columns

3️⃣ Feature Engineering

Created derived columns:

Avg_Salary

Salary_Spread

Salary_Band

Seniority_Level

Degree_Level

Posting_Year

Job_Posting_Age

Text fields were cleaned and parsed to extract skills and qualification levels.


## Visualization

Matplotlib used for static charts

Plotly used for interactive dashboards

Spark DataFrames converted to Pandas for plotting


## How to Run

Install dependencies

pip install -r requirements.txt


Ensure Java 11 is installed and JAVA_HOME is set

Run pipeline

python main_pipeline.py


## Performance Considerations

Column pruning applied during cleaning

Filtering performed before aggregations

Avoided large .toPandas() conversions

Used Spark distributed processing for aggregations


## Future Enhancements

Deploy pipeline using Airflow

Store output in AWS S3 / Data Lake

Add automated testing suite

Implement Docker containerization

Build BI dashboard integration