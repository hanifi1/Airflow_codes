# import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow.models import DAG


# Operators
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago



default_args = {
    'owner': 'Mahdi',
    'start_date': days_ago(0),     ##   today
    'email': ['mahdi@dummy.com'],
    'email_on_failure': True,
    'email_on_retry' : True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# define the DAG
dag = DAG(
    dag_id='ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)