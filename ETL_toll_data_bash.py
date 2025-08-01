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


unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command= 'tar -xvzf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment/',
    dag=dag,
)

extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command="""
    cut -d',' -f1,2,3,4 /home/project/airflow/dags/finalassignment/vehicle-data.csv > /home/project/airflow/dags/finalassignment/csv_data.csv
    """,
    dag=dag
)

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command="""
    cut -f5,6,7 --output-delimiter=',' /home/project/airflow/dags/finalassignment/tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/tsv_data.csv
    """,
    dag=dag
)

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command="""
           awk '{print substr($0,length($0)-8, 3) "," substr($0, length($0)-4, 5)}' \
           /home/project/airflow/dags/finalassignment/payment-data.txt > /home/project/airflow/dags/finalassignment/fixed_width_data.csv
    """,
    dag=dag
)




consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command="""
    paste -d',' \
    /home/project/airflow/dags/finalassignment/csv_data.csv \
    /home/project/airflow/dags/finalassignment/tsv_data.csv \
    /home/project/airflow/dags/finalassignment/fixed_width_data.csv \
    > /home/project/airflow/dags/finalassignment/extracted_data.csv
    """,
    dag=dag
)


transform_data = BashOperator(
    task_id='transform_data',
    bash_command="""
    mkdir -p /home/project/airflow/dags/finalassignment/staging && \
    awk -F',' 'BEGIN {OFS=","} { $4 = toupper($4); print }' \
    /home/project/airflow/dags/finalassignment/extracted_data.csv \
    > /home/project/airflow/dags/finalassignment/staging/transformed_data.csv
    """,
    dag=dag 
)

unzip_data >> [extract_data_from_csv, extract_data_from_tsv, extract_data_from_fixed_width]
[extract_data_from_csv, extract_data_from_tsv, extract_data_from_fixed_width] >> consolidate_data
consolidate_data >> transform_data