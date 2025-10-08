from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# Create the DAG
dag = DAG(
    'weather_data_processing_pipeline_a2' \
    '',
    default_args=default_args,
    description='Daily batch processing of weather data using Spark',
    schedule_interval='@daily',  # Run daily at midnight
    catchup=False,  # Don't run for past dates
    max_active_runs=1,  # Only one instance running at a time
    tags=['weather', 'batch', 'spark', 'etl'],
)

def validate_input_data(**context):
    """
    Validate that input data exists and has expected format
    """
    import os
    import pandas as pd
    
    data_path = '/opt/airflow/data/raw'
    required_files = []
    
    # Check if data directory exists and has CSV files
    if not os.path.exists(data_path):
        raise FileNotFoundError(f"Data directory {data_path} not found")
    
    csv_files = [f for f in os.listdir(data_path) if f.endswith('.csv')]
    
    if not csv_files:
        logging.warning("No CSV files found in data directory")
        # For demo purposes, create sample data
        create_sample_data()
    
    logging.info(f"Found {len(csv_files)} CSV files for processing")
    return True

def create_sample_data():
    """
    Create sample weather data for demonstration
    """
    import pandas as pd
    import numpy as np
    from datetime import datetime, timedelta
    import os
    
    cities = ['Delhi', 'Mumbai', 'Bangalore', 'Chennai', 'Kolkata', 
              'Hyderabad', 'Pune', 'Ahmedabad', 'Jaipur', 'Lucknow']
    
    # Create 25 years of daily data
    start_date = datetime(1999, 1, 1)
    end_date = datetime(2024, 12, 31)
    date_range = pd.date_range(start_date, end_date, freq='D')
    
    data = []
    np.random.seed(42)  # For reproducible data
    
    for city in cities:
        for date in date_range:
            # Simulate seasonal temperature variations
            day_of_year = date.timetuple().tm_yday
            base_temp = 20 + 10 * np.sin(2 * np.pi * day_of_year / 365)
            
            # Add city-specific temperature adjustments
            city_adjustments = {
                'Delhi': 5, 'Mumbai': 8, 'Chennai': 10, 'Bangalore': -2,
                'Kolkata': 3, 'Hyderabad': 6, 'Pune': 4, 'Ahmedabad': 7,
                'Jaipur': 6, 'Lucknow': 4
            }
            
            temperature = base_temp + city_adjustments[city] + np.random.normal(0, 3)
            
            # Simulate rainfall (higher during monsoon months)
            if 6 <= date.month <= 9:  # Monsoon season
                rainfall = max(0, np.random.exponential(5))
            else:
                rainfall = max(0, np.random.exponential(0.5))
            
            # Simulate humidity
            humidity = max(30, min(95, 60 + np.random.normal(0, 15)))
            
            # Occasionally introduce missing values (2% chance)
            if np.random.random() < 0.02:
                temperature = None
            if np.random.random() < 0.02:
                rainfall = None
            if np.random.random() < 0.02:
                humidity = None
            
            data.append({
                'Date': date.strftime('%Y-%m-%d'),
                'City': city,
                'Temperature': temperature,
                'Rainfall': rainfall,
                'Humidity': humidity,
                'WindSpeed': max(0, np.random.normal(10, 5)),
                'Pressure': np.random.normal(1013, 20)
            })
    
    df = pd.DataFrame(data)
    
    # Save to multiple files (one per city for better partitioning)
    os.makedirs('/opt/airflow/data/raw', exist_ok=True)
    
    for city in cities:
        city_data = df[df['City'] == city]
        city_data.to_csv(f'/opt/airflow/data/raw/weather_{city.lower()}.csv', index=False)
    
    logging.info(f"Created sample weather data for {len(cities)} cities")

def check_data_quality(**context):
    """
    Perform basic data quality checks
    """
    import os
    import pandas as pd
    
    data_path = '/opt/airflow/data/raw'
    csv_files = [f for f in os.listdir(data_path) if f.endswith('.csv')]
    
    total_rows = 0
    issues = []
    
    for file in csv_files:
        df = pd.read_csv(os.path.join(data_path, file))
        total_rows += len(df)
        
        # Check for required columns
        required_columns = ['Date', 'City', 'Temperature', 'Rainfall', 'Humidity']
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            issues.append(f"{file}: Missing columns {missing_columns}")
        
        # Check for excessive missing values
        for col in required_columns:
            if col in df.columns:
                missing_pct = df[col].isna().sum() / len(df) * 100
                if missing_pct > 10:  # More than 10% missing
                    issues.append(f"{file}: {col} has {missing_pct:.1f}% missing values")
    
    logging.info(f"Data quality check completed. Total rows: {total_rows}")
    
    if issues:
        logging.warning(f"Data quality issues found: {issues}")
        # For demo purposes, we'll continue even with issues
        # In production, you might want to fail the task
    
    return {'total_rows': total_rows, 'issues': issues}

def post_processing_validation(**context):
    """
    Validate that the processing completed successfully
    """
    import os
    
    output_path = '/opt/airflow/data/processed'
    
    if not os.path.exists(output_path):
        raise FileNotFoundError("Processed data directory not found")
    
    output_files = os.listdir(output_path)
    
    if not output_files:
        raise FileNotFoundError("No processed files found")
    
    logging.info(f"Processing completed successfully. Output files: {output_files}")
    return True

# Task 1: Validate input data
validate_data_task = PythonOperator(
    task_id='validate_input_data',
    python_callable=validate_input_data,
    dag=dag,
    doc_md="Validates that input data exists and creates sample data if needed"
)

# Task 2: Data quality checks
quality_check_task = PythonOperator(
    task_id='data_quality_check',
    python_callable=check_data_quality,
    dag=dag,
    doc_md="Performs basic data quality validation checks"
)

# Task 3: Create output directory
create_output_dir = BashOperator(
    task_id='create_output_directory',
    bash_command='mkdir -p /opt/airflow/data/processed',
    dag=dag,
)

# Task 4: Spark job for data processing
spark_processing_task = SparkSubmitOperator(
    task_id='spark_weather_processing',
    application='/opt/spark/jobs/spark_job.py',
    conn_id='spark_default',
    verbose=True,
    application_args=[
        '--input_path', '/opt/airflow/data/raw',
        '--output_path', '/opt/airflow/data/processed',
        '--execution_date', '{{ ds }}'
    ],
    conf={
        
        'spark.executor.memory': '1g',
        'spark.executor.cores': '1',
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true',
    },
    dag=dag,
    retries=2,
    retry_delay=timedelta(minutes=2),
    doc_md="Main Spark job for processing weather data"
)

# Task 5: Post-processing validation
validation_task = PythonOperator(
    task_id='post_processing_validation',
    python_callable=post_processing_validation,
    dag=dag,
    doc_md="Validates that processing completed successfully"
)

# Task 6: Cleanup temporary files (optional)
cleanup_task = BashOperator(
    task_id='cleanup_temp_files',
    bash_command='''
    # Clean up temporary files older than 7 days
    find /opt/airflow/data/processed -name "*.tmp" -mtime +7 -delete
    find /opt/airflow/logs -name "*.log" -mtime +30 -delete
    echo "Cleanup completed"
    ''',
    dag=dag,
    trigger_rule='none_failed_min_one_success'  # Run even if some upstream tasks fail
)

# Define task dependencies
validate_data_task >> quality_check_task >> create_output_dir
create_output_dir >> spark_processing_task >> validation_task >> cleanup_task

# Add task documentation
dag.doc_md = """
# Weather Data Processing Pipeline

This DAG processes daily weather data for 10 Indian cities using Apache Spark.

## Pipeline Steps:
1. **Validate Input Data**: Checks if input data exists, creates sample data if needed
2. **Data Quality Check**: Performs basic validation on data completeness and format
3. **Create Output Directory**: Ensures output directory exists
4. **Spark Processing**: Main data processing using PySpark
5. **Post-processing Validation**: Verifies processing completed successfully
6. **Cleanup**: Removes old temporary files

## Configuration:
- **Schedule**: Daily at midnight
- **Retries**: 3 attempts with 5-minute delays
- **Timeout**: 2 hours maximum execution time
- **Concurrency**: Only one instance runs at a time

## Monitoring:
- Check Airflow UI for task status and logs
- Spark UI available at localhost:9090
- Output data stored in `/opt/airflow/data/processed/`
"""