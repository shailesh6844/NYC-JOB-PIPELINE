# Running Airflow with Docker

This guide explains how to run Apache Airflow using Docker on Windows.

## Prerequisites

- **Docker Desktop** installed and running
- At least 4GB of RAM allocated to Docker
- At least 2 CPUs allocated to Docker

## Quick Start

### 1. Create Required Directories

```powershell
# Create directories for logs, plugins, and config
New-Item -ItemType Directory -Force -Path logs, plugins, config
```

### 2. Start Airflow

```powershell
# Start all Airflow services
docker-compose -f docker-compose-airflow.yml --env-file .env.airflow up -d
```

This will:
- Initialize the Airflow database (PostgreSQL)
- Create admin user (username: `admin`, password: `admin`)
- Start the webserver, scheduler, and triggerer

### 3. Access Airflow UI

- **URL:** http://localhost:8080
- **Username:** `admin`
- **Password:** `admin`

### 4. Trigger the Pipeline

1. In the Airflow UI, find the `nyc_jobs_pipeline` DAG
2. Toggle it to "ON" (unpause)
3. Click the play button to trigger a run
4. Monitor progress in the Graph or Grid view

## Managing Airflow

### View Logs
```powershell
# View all logs
docker-compose -f docker-compose-airflow.yml --env-file .env.airflow logs

# View specific service logs
docker-compose -f docker-compose-airflow.yml --env-file .env.airflow logs airflow-scheduler
```

### Stop Airflow
```powershell
docker-compose -f docker-compose-airflow.yml --env-file .env.airflow down
```

### Stop and Remove All Data
```powershell
# WARNING: This deletes the database and all DAG run history
docker-compose -f docker-compose-airflow.yml --env-file .env.airflow down -v
```

### Restart Airflow
```powershell
docker-compose -f docker-compose-airflow.yml --env-file .env.airflow restart
```

## DAG Information

**DAG Name:** `nyc_jobs_pipeline`

**Tasks:**
1. `initialize_spark` - Initialize Spark session
2. `load_data` - Load CSV data
3. `clean_data` - Clean the data
4. `save_cleaned_data` - Save cleaned data as CSV
5. `feature_engineering` - Apply feature engineering
6. `build_kpis` - Generate 6 KPIs
7. `generate_visualizations` - Create PNG visualizations
8. `save_processed_data` - Save final processed data
9. `cleanup` - Stop Spark session

**Schedule:** Daily at midnight (configurable in DAG file)

## Output Files

All outputs are saved to the `output/` directory:
- `output/cleaned_jobs_csv/` - Cleaned data in CSV format
- `output/processed_jobs/` - Final processed data in Parquet format
- `output/visualizations/` - 6 KPI visualization images

## Troubleshooting

### Port 8080 Already in Use
If port 8080 is already in use, edit `docker-compose-airflow.yml` and change:
```yaml
ports:
  - "8081:8080"  # Change 8081 to any available port
```

### DAG Not Appearing
1. Check that the `dags/` folder is mounted correctly
2. Check scheduler logs: `docker-compose -f docker-compose-airflow.yml --env-file .env.airflow logs airflow-scheduler`
3. Verify DAG file has no syntax errors

### Memory Issues
Increase Docker Desktop memory allocation:
1. Open Docker Desktop
2. Go to Settings → Resources
3. Increase Memory to at least 4GB

## Notes

- The Docker setup uses Airflow 2.10.4 with Python 3.12
- All project dependencies (PySpark, matplotlib, pandas, plotly) are installed automatically
- The setup uses PostgreSQL as the metadata database (more robust than SQLite)
- Logs are persisted in the `logs/` directory
