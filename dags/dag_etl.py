from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from extract import fetch_data
from transform import transform
from load import load_csv_to_postgres, get_postgres_engine

# Get the Postgres engine
engine = get_postgres_engine()

# Define a function to debug the environment
def debug_environment():
    import sys
    import subprocess
    import os

    print("Python Path:", sys.path)
    print("Installed Packages:")
    subprocess.run(["pip", "list"], check=True)

    print("Environment Variables:")
    for key, value in os.environ.items():
        print(f"{key}: {value}")

# Define the default arguments
default_args = {
    'owner': 'greg',
    'start_date': datetime(year=2024, month=11, day=13),
    'email': 'gregorytsado@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False
}

# Define the DAG
with DAG(
    'my_dag',
    default_args=default_args,
    description='ETL pipeline for gold fintech',
    schedule_interval='0 0 * * 4',
    catchup=True
) as dag: 

    # Start task
    start_task = DummyOperator(
        task_id='Start_pipeline'
    )

    # Debug task to check the environment
    debug_task = PythonOperator(
        task_id='debug_environment',
        python_callable=debug_environment
    )

    # Extract task
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=fetch_data
    )

    # Transform task
    transform_task = SparkSubmitOperator(
        task_id='transform_task',
        application='transform.py',  # Path to your Spark notebook or script
        name='goldfintech-transform',
        conn_id='spark_default',  # Ensure this Airflow connection is configured
        application_args=[],  # Add arguments if needed
        verbose=True,
        conf={
            'spark.driver.memory': '1g',
            'spark.executor.memory': '1g',
        },
        env_vars={
            'JAVA_HOME': '/opt/bitnami/java',
            'SPARK_HOME': '/opt/bitnami/spark',
        }

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
    start_task >> debug_task >> extract_task >> transform_task >> load_task >> end_task
