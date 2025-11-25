from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
import random

# Default arguments
default_args = {
    'owner': 'analytics-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

# Define the DAG
dag = DAG(
    'advanced_data_processing',
    default_args=default_args,
    description='Advanced data processing with branching and sensors',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    catchup=False,
    tags=['advanced', 'analytics', 'etl'],
    max_active_runs=1,
)

# Python functions
def check_data_source(**context):
    """Check which data source to process"""
    # Simulate random data source selection
    sources = ['database', 'api', 'file']
    selected_source = random.choice(sources)
    print(f"Selected data source: {selected_source}")
    return f"process_{selected_source}_data"

def process_database_data(**context):
    """Process data from database"""
    print("Processing database data...")
    # Simulate database processing
    records = random.randint(1000, 5000)
    print(f"Processed {records} records from database")
    return {'source': 'database', 'records': records}

def process_api_data(**context):
    """Process data from API"""
    print("Processing API data...")
    # Simulate API processing
    records = random.randint(500, 2000)
    print(f"Processed {records} records from API")
    return {'source': 'api', 'records': records}

def process_file_data(**context):
    """Process data from file"""
    print("Processing file data...")
    # Simulate file processing
    records = random.randint(800, 3000)
    print(f"Processed {records} records from file")
    return {'source': 'file', 'records': records}

def aggregate_data(**context):
    """Aggregate processed data"""
    print("Aggregating data from all sources...")
    ti = context['ti']
    
    # Try to get data from all possible upstream tasks
    total_records = 0
    sources_processed = []
    
    for task_id in ['process_database_data', 'process_api_data', 'process_file_data']:
        try:
            data = ti.xcom_pull(task_ids=task_id)
            if data:
                total_records += data['records']
                sources_processed.append(data['source'])
        except:
            pass
    
    print(f"Total records aggregated: {total_records}")
    print(f"Sources processed: {sources_processed}")
    return {'total_records': total_records, 'sources': sources_processed}

def data_quality_check(**context):
    """Perform data quality checks"""
    ti = context['ti']
    aggregated_data = ti.xcom_pull(task_ids='aggregate_data')
    
    total_records = aggregated_data['total_records']
    
    # Simple quality check
    if total_records < 100:
        raise ValueError(f"Data quality check failed: too few records ({total_records})")
    
    print(f"Data quality check passed: {total_records} records")
    return True

def handle_failure(**context):
    """Handle pipeline failure"""
    print("Pipeline failed! Sending alerts and cleaning up...")
    # Add failure handling logic here
    return "failure_handled"

# Define tasks
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

# File sensor to wait for trigger file
wait_for_trigger = FileSensor(
    task_id='wait_for_trigger_file',
    filepath='/tmp/trigger.txt',
    fs_conn_id='fs_default',
    poke_interval=30,
    timeout=300,
    dag=dag,
)

# Branch operator to decide data source
branch_task = BranchPythonOperator(
    task_id='check_data_source',
    python_callable=check_data_source,
    dag=dag,
)

# Data processing tasks for different sources
process_db_task = PythonOperator(
    task_id='process_database_data',
    python_callable=process_database_data,
    dag=dag,
)

process_api_task = PythonOperator(
    task_id='process_api_data',
    python_callable=process_api_data,
    dag=dag,
)

process_file_task = PythonOperator(
    task_id='process_file_data',
    python_callable=process_file_data,
    dag=dag,
)

# Join point - will run regardless of which branch was taken
aggregate_task = PythonOperator(
    task_id='aggregate_data',
    python_callable=aggregate_data,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag,
)

# Data quality check
quality_check_task = PythonOperator(
    task_id='data_quality_check',
    python_callable=data_quality_check,
    dag=dag,
)

# Generate report
generate_report_task = BashOperator(
    task_id='generate_report',
    bash_command='''
    echo "Generating data processing report..."
    echo "Report generated at: $(date)"
    echo "Report saved to: /tmp/daily_report_$(date +%Y%m%d).txt"
    ''',
    dag=dag,
)

# Failure handling task
failure_task = PythonOperator(
    task_id='handle_failure',
    python_callable=handle_failure,
    trigger_rule=TriggerRule.ONE_FAILED,
    dag=dag,
)

# Success notification
success_task = BashOperator(
    task_id='success_notification',
    bash_command='echo "Pipeline completed successfully!"',
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

end_task = DummyOperator(
    task_id='end',
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag,
)

# Define task dependencies
start_task >> wait_for_trigger >> branch_task

# Branching paths
branch_task >> [process_db_task, process_api_task, process_file_task]

# All branches converge to aggregate
[process_db_task, process_api_task, process_file_task] >> aggregate_task

# Main pipeline flow
aggregate_task >> quality_check_task >> generate_report_task >> success_task >> end_task

# Failure handling
[aggregate_task, quality_check_task, generate_report_task] >> failure_task >> end_task