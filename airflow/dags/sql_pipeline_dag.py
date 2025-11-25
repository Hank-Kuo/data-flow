from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

# Default arguments
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'sql_data_pipeline',
    default_args=default_args,
    description='SQL-based data processing pipeline',
    schedule_interval='0 1 * * *',  # Run daily at 1 AM
    catchup=False,
    tags=['sql', 'etl', 'database'],
)

def execute_sql_query(query_name, sql_query):
    """Execute SQL query and return results"""
    def _execute(**context):
        print(f"Executing {query_name}...")
        print(f"SQL Query: {sql_query}")
        
        # Simulate SQL execution
        print(f"Query executed successfully!")
        return f"{query_name}_completed"
    
    return _execute

# Sample SQL queries
CREATE_STAGING_TABLE = """
CREATE TABLE IF NOT EXISTS staging_sales (
    id SERIAL PRIMARY KEY,
    customer_id INT,
    product_id INT,
    quantity INT,
    price DECIMAL(10,2),
    sale_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

EXTRACT_DAILY_SALES = """
INSERT INTO staging_sales (customer_id, product_id, quantity, price, sale_date)
SELECT 
    customer_id,
    product_id,
    quantity,
    price,
    CURRENT_DATE as sale_date
FROM raw_sales 
WHERE DATE(created_at) = CURRENT_DATE;
"""

CALCULATE_METRICS = """
CREATE TABLE IF NOT EXISTS daily_metrics AS
SELECT 
    sale_date,
    COUNT(*) as total_transactions,
    SUM(quantity * price) as total_revenue,
    AVG(quantity * price) as avg_transaction_value,
    COUNT(DISTINCT customer_id) as unique_customers
FROM staging_sales
WHERE sale_date = CURRENT_DATE
GROUP BY sale_date;
"""

CLEANUP_OLD_DATA = """
DELETE FROM staging_sales 
WHERE created_at < CURRENT_DATE - INTERVAL '7 days';
"""

# Define tasks
start_task = DummyOperator(
    task_id='start_sql_pipeline',
    dag=dag,
)

# Database preparation
create_tables_task = PythonOperator(
    task_id='create_staging_tables',
    python_callable=execute_sql_query('Create Staging Tables', CREATE_STAGING_TABLE),
    dag=dag,
)

# Data extraction
extract_data_task = PythonOperator(
    task_id='extract_daily_sales',
    python_callable=execute_sql_query('Extract Daily Sales', EXTRACT_DAILY_SALES),
    dag=dag,
)

# Data validation
validate_data_task = BashOperator(
    task_id='validate_extracted_data',
    bash_command='''
    echo "Validating extracted data..."
    echo "Checking data quality and completeness..."
    echo "Validation completed successfully!"
    ''',
    dag=dag,
)

# Calculate business metrics
calculate_metrics_task = PythonOperator(
    task_id='calculate_daily_metrics',
    python_callable=execute_sql_query('Calculate Daily Metrics', CALCULATE_METRICS),
    dag=dag,
)

# Generate business report
generate_report_task = BashOperator(
    task_id='generate_business_report',
    bash_command='''
    echo "Generating daily business report..."
    echo "Date: $(date)"
    echo "Report includes:"
    echo "- Total transactions"
    echo "- Total revenue"
    echo "- Average transaction value"
    echo "- Unique customers"
    echo "Report generation completed!"
    ''',
    dag=dag,
)

# Data cleanup
cleanup_task = PythonOperator(
    task_id='cleanup_old_data',
    python_callable=execute_sql_query('Cleanup Old Data', CLEANUP_OLD_DATA),
    dag=dag,
)

# Archive data
archive_task = BashOperator(
    task_id='archive_processed_data',
    bash_command='''
    echo "Archiving processed data..."
    echo "Creating backup of daily metrics..."
    echo "Data archived successfully!"
    ''',
    dag=dag,
)

end_task = DummyOperator(
    task_id='end_sql_pipeline',
    dag=dag,
)

# Define task dependencies
start_task >> create_tables_task >> extract_data_task >> validate_data_task
validate_data_task >> calculate_metrics_task >> generate_report_task
generate_report_task >> [cleanup_task, archive_task] >> end_task