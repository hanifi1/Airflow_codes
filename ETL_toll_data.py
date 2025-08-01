# import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow.models import DAG

import os
import requests
import tarfile


# Operators
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago

url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"
destination_dir = "/home/project/airflow/dags/python_etl/staging"

def download_dataset():
    
    destination_file = os.path.join(destination_dir, "tolldata.tgz")

    # Create destination directory if it doesn't exist
    os.makedirs(destination_dir, exist_ok=True)

    # Download the file
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(destination_file, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
        print(f"Download completed: {destination_file}")
    else:
        raise Exception(f"Failed to download file, status code: {response.status_code}")

  

def untar_dataset():

    tar_file_path = os.path.join(destination_dir, "tolldata.tgz")
    extract_path = "/home/project/airflow/dags/python_etl/staging"


    # Check if the tar file exists
    if not os.path.exists(tar_file_path):
        raise FileNotFoundError(f"{tar_file_path} does not exist.")

    # Extract the tar.gz file
    with tarfile.open(tar_file_path, "r:gz") as tar:
        tar.extractall(path=extract_path)
        print(f"Extracted files to: {extract_path}")



def extract_data_from_csv():
    input_path = f"{destination_dir}/vehicle-data.csv"
    out_path = f"{destination_dir}/csv_data.csv"
    df = pd.read_csv(input_path, header=None)
    df = df[[0,1,2,3]]
    df.columns = ['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type']
    df.to_csv(out_path, index=False)

def extract_data_from_tsv():
    input_path = f"{destination_dir}/tollplaza-data.tsv"
    out_path = f"{destination_dir}/tsv_data.csv"
    df = pd.read_csv(input_path, sep = '\t' ,header=None)
    df = df[[4,5,6]]
    df.columns = ['Number of axles', 'Tollplaza id', 'Tollplaza code']
    df.to_csv(out_path, index=False)

def extract_data_from_fixed_width():
    input_path = f"{destination_dir}/payment-data.txt"
    out_path = f"{destination_dir}/fixed_width_data.csv"
    df = pd.read_fwf(input_path, header=None)
    df = df[[9, 10]]
    df.columns = ['Type of Payment code', 'Vehicle Code']
    df.to_csv(out_path, index=False)

def consolidate_data():
    path_to_csv  = f'{destination_dir}/csv_data.csv'
    path_to_tsv = f'{destination_dir}/tsv_data.csv'
    path_to_fixed_width = f'{destination_dir}/fixed_width_data.csv'

    out_path = f'{destination_dir}/extracted_data.csv'

    df_csv = pd.read_csv(path_to_csv)
    df_tsv = pd.read_csv(path_to_tsv)
    df_fixed_width = pd.read_csv(path_to_fixed_width)

    df = pd.concat([df_csv, df_tsv, df_fixed_width], axis=1)
    df.to_csv(out_path, index=False)

def transform_data():
    path_in = f'{destination_dir}/extracted_data.csv'
    out_path = f'{destination_dir}/transformed_data.csv'
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

download_datasets = PythonOperator(
    task_id='download_datasets',
    python_callable=download_dataset,
    dag=dag,
)

untar_datasets = PythonOperator(
    task_id='untar_datasets',
    python_callable=untar_dataset,
    dag=dag,
)
extract_csv = PythonOperator(
    task_id='extract_csv',
    python_callable=extract_data_from_csv,
    dag=dag,
)
extract_tsv = PythonOperator(
    task_id='extract_tsv',
    python_callable=extract_data_from_tsv,
    dag=dag,
)
extract_fixed_width = PythonOperator(
    task_id='extract_fixed_width',
    python_callable=extract_data_from_fixed_width,
    dag=dag,
)
consolidate_data = PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate_data,
    dag=dag,
)
transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

# Set the task

download_datasets >> untar_datasets >> [extract_csv, extract_tsv, extract_fixed_width] >> consolidate_data >> transform_data