# Apache Airflow Setup Guide

This guide explains how to set up and run Apache Airflow to orchestrate the NYC Jobs data pipeline.

## Prerequisites

- Python 3.8+
- All project dependencies installed
- JAVA_HOME and HADOOP_HOME environment variables configured

## Installation

1. **Install Airflow** (if not already installed):
```bash
uv pip install apache-airflow
```

2. **Set Airflow Home** (optional, defaults to `~/airflow`):
```powershell
$env:AIRFLOW_HOME = "C:\Users\shailesh shinde\OneDrive\Desktop\Dubai Assignment\airflow"
```

## Setup

1. **Initialize Airflow Database**:
```bash
airflow db init
```

2. **Create an Admin User**:
```bash
airflow users create `
    --username admin `
    --firstname Admin `
    --lastname User `
    --role Admin `
    --email admin@example.com `
    --password admin
```

## Running Airflow

### Option 1: Standalone Mode (Recommended for Windows)

This is the simplest way to run Airflow - it starts all components in one process:

```bash
$env:AIRFLOW_HOME = "C:\Users\shailesh shinde\OneDrive\Desktop\Dubai Assignment\airflow"
uv run airflow standalone
```

The command will output the admin password. Access the UI at http://localhost:8080

### Option 2: Separate Components

**Start the Airflow API Server** (in one terminal):
```bash
$env:AIRFLOW_HOME = "C:\Users\shailesh shinde\OneDrive\Desktop\Dubai Assignment\airflow"
uv run airflow api-server
```

**Start the Airflow Scheduler** (in another terminal):
```bash
$env:AIRFLOW_HOME = "C:\Users\shailesh shinde\OneDrive\Desktop\Dubai Assignment\airflow"
uv run airflow scheduler
```

**Access the Airflow UI**:
   - Open browser: http://localhost:8080
   - Login with username: `admin`, password: `admin`

## Using the DAG

1. **Locate the DAG**:
   - The DAG `nyc_jobs_pipeline` should appear in the Airflow UI
   - If not visible, check the DAG file for syntax errors

2. **Trigger the DAG**:
   - Click on the DAG name
   - Click the "Play" button to trigger a manual run
   - Or wait for the scheduled run (daily at midnight)

3. **Monitor Progress**:
   - View task status in the Graph or Tree view
   - Check logs for each task by clicking on the task box

## DAG Tasks

The pipeline consists of the following tasks:

1. **initialize_spark**: Initialize Spark session
2. **load_data**: Load raw data from CSV
3. **clean_data**: Clean the loaded data
4. **save_cleaned_data**: Save cleaned data as CSV
5. **feature_engineering**: Apply feature engineering
6. **build_kpis**: Generate all 6 KPIs
7. **generate_visualizations**: Create and save visualization images
8. **save_processed_data**: Save final processed data
9. **cleanup**: Stop Spark session

## Configuration

- **Schedule**: Daily (`@daily`)
- **Start Date**: 2026-02-14
- **Retries**: 1
- **Retry Delay**: 5 minutes

To change the schedule, edit `schedule_interval` in `dags/nyc_jobs_pipeline_dag.py`.

## Troubleshooting

- **DAG not appearing**: Check `dags_folder` in `airflow.cfg` points to your `dags` directory
- **Import errors**: Ensure project root is in Python path
- **Spark errors**: Verify JAVA_HOME and HADOOP_HOME are set correctly
