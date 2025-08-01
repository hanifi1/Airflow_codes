# import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
# from airflow.models import DAG

import os
import requests
import tarfile


# Operators
# from airflow.operators.bash_operator import BashOperator
# from airflow.operators.python import PythonOperator
# from airflow.operators.email import EmailOperator
# from airflow.utils.dates import days_ago



# default_args = {
#     'owner': 'Mahdi',
#     'start_date': days_ago(0),     ##   today
#     'email': ['mahdi@dummy.com'],
#     'email_on_failure': True,
#     'email_on_retry' : True,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }


# # define the DAG
# dag = DAG(
#     dag_id='ETL_toll_data',
#     default_args=default_args,
#     description='Apache Airflow Final Assignment',
#     schedule_interval=timedelta(days=1),
# )




def download_dataset():
    url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"
    # destination_dir = "/home/project/airflow/dags/python_etl/staging"
    destination_dir = '/Users/mahdihanifi/Documents/GitHub/Apache_Airflow/staging'
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
    # tar_file_path = "/home/project/airflow/dags/python_etl/staging/tolldata.tgz"
    # extract_path = "/home/project/airflow/dags/python_etl/staging"

    tar_file_path = "/Users/mahdihanifi/Documents/GitHub/Apache_Airflow/staging/tolldata.tgz"
    extract_path = "/Users/mahdihanifi/Documents/GitHub/Apache_Airflow/staging"

    # Check if the tar file exists
    if not os.path.exists(tar_file_path):
        raise FileNotFoundError(f"{tar_file_path} does not exist.")

    # Extract the tar.gz file
    with tarfile.open(tar_file_path, "r:gz") as tar:
        tar.extractall(path=extract_path)
        print(f"Extracted files to: {extract_path}")
    

if __name__ == "__main__":
    # download_dataset()
    untar_dataset()

# path = /Users/mahdihanifi/Documents/GitHub/Apache_Airflow/staging