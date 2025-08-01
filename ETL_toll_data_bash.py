# import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow.models import DAG


# Operators
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago

def extract_data_from_csv():
    input_path = f"{path_to_file}/vehicle-data.csv"
    out_path = f"{path_to_file}/out/csv_data.csv"
    df = pd.read_csv(input_path, header=None)
    df = df[[0,1,2,3]]
    df.columns = ['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type']
    df.to_csv(out_path, index=False)

def extract_data_from_tsv():
    input_path = f"{path_to_file}/tollplaza-data.tsv"
    out_path = f"{path_to_file}/out/tsv_data.csv"
    df = pd.read_csv(input_path, sep = '\t' ,header=None)
    df = df[[4,5,6]]
    df.columns = ['Number of axles', 'Tollplaza id', 'Tollplaza code']
    df.to_csv(out_path, index=False)

def extract_data_from_fixed_width():
    input_path = f"{path_to_file}/payment-data.txt"
    out_path = f"{path_to_file}/out/fixed_width_data.csv"
    df = pd.read_fwf(input_path, header=None)
    df = df[[9, 10]]
    df.columns = ['Type of Payment code', 'Vehicle Code']
    df.to_csv(out_path, index=False)

def consolidate_data():
    path_to_csv  = f'{path_to_file}/out/csv_data.csv'
    path_to_tsv = f'{path_to_file}/out/tsv_data.csv'
    path_to_fixed_width = f'{path_to_file}/out/fixed_width_data.csv'

    out_path = f'{path_to_file}/out/extracted_data.csv'

    df_csv = pd.read_csv(path_to_csv)
    df_tsv = pd.read_csv(path_to_tsv)
    df_fixed_width = pd.read_csv(path_to_fixed_width)

    df = pd.concat([df_csv, df_tsv, df_fixed_width], axis=1)
    df.to_csv(out_path, index=False)

def transform_data():
    path_in = f'{path_to_file}/out/extracted_data.csv'
    out_path = f'{path_to_file}/out/transformed_data.csv'
    df = pd.read_csv(path_in)
    df['Vehicle type'] = df['Vehicle type'].map(lambda x: x.upper())
    df.to_csv( out_path, index=False)



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

unzip_data >> [extract_data_from_csv, extract_data_from_tsv, extract_data_from_fixed_width] >> consolidate_data >> transform_data





# 00848875
# Hani1979