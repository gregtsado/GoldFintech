from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from extract import fetch_data
from data_quality_checks import data_quality
from transfrom_pan import transform_fun
from load import load_csv_to_postgres, get_postgres_engine

# Get the Postgres engine
engine = get_postgres_engine()

# Define the default arguments
default_args = {
    'owner': 'greg',
    'start_date': datetime(year=2024, month=11, day=13),
    'email': 'gregstyles68@gmail.com',
    'email_on_failure': True,
    'email_on_retry': False
}

# Define the DAG
with DAG(
    'my_dag',
    default_args=default_args,
    description='ETL pipeline for gold fintech',
    schedule_interval='0 0 * * 4',
    catchup=False
) as dag: 

    # Start task
    start_task = DummyOperator(
        task_id='Start_pipeline'

    )

    # Extract task
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=fetch_data
    )
    

    # Transform task
    data_quality_task = PythonOperator(
        task_id='data_quality_checks',
        python_callable=data_quality
    )

    # Transform task
    transform_task = PythonOperator(
        task_id='transform_pan',
        python_callable=transform_fun
    )

    # Load task
    load_task = PythonOperator(
        task_id='load',
        python_callable=load_csv_to_postgres,
        op_kwargs={'table_name': 'goldfintech', 'engine': engine}
    )

    # End task
    end_task = DummyOperator(
        task_id='end_pipeline'
    )

    # Set up task dependencies
    start_task >>  extract_task >> data_quality_task >> transform_task >>load_task >> end_task
