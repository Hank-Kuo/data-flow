from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

# Default arguments for the DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'example_data_pipeline',
    default_args=default_args,
    description='A simple data pipeline example',
    schedule_interval=timedelta(days=1),  # Run daily
    catchup=False,
    tags=['example', 'data-pipeline'],
)

# Python function for data extraction
def extract_data(**context):
    """Extract data from source"""
    print("Extracting data from source...")
    # Simulate data extraction
    data = {'records': 1000, 'timestamp': str(datetime.now())}
    print(f"Extracted {data['records']} records at {data['timestamp']}")
    return data

# Python function for data transformation
def transform_data(**context):
    """Transform extracted data"""
    print("Transforming data...")
    # Get data from previous task
    ti = context['ti']
    extracted_data = ti.xcom_pull(task_ids='extract_data')
    
    # Simulate transformation
    transformed_records = extracted_data['records'] * 0.95  # 5% data cleaning
    print(f"Transformed {transformed_records} records")
    return {'cleaned_records': transformed_records}

# Python function for data loading
def load_data(**context):
    """Load transformed data to destination"""
    print("Loading data to destination...")
    ti = context['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_data')
    
    # Simulate loading
    print(f"Loading {transformed_data['cleaned_records']} records to data warehouse")
    print("Data loading completed successfully!")

# Define tasks
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

# Bash task for data validation
validate_task = BashOperator(
    task_id='validate_data',
    bash_command='echo "Validating data quality..." && echo "Data validation passed!"',
    dag=dag,
)

# Notification task
notify_task = BashOperator(
    task_id='send_notification',
    bash_command='echo "Pipeline completed successfully! Sending notification..."',
    dag=dag,
)

end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# Define task dependencies
start_task >> extract_task >> transform_task >> load_task >> validate_task >> notify_task >> end_task