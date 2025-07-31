import os
import requests
import tarfile
import pandas as pd


def download_dataset():
    url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"
    # destination_dir = "/home/project/airflow/dags/python_etl/staging"
    destination_dir = "./staging"
    destination_path = os.path.join(destination_dir, "tolldata.tgz")

    # Create destination directory if it doesn't exist
    os.makedirs(destination_dir, exist_ok=True)

    print("Starting download...")
    response = requests.get(url, stream=True)
    
    if response.status_code == 200:
        with open(destination_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        print(f"Download completed: {destination_path}")
    else:
        raise Exception(f"Failed to download file. Status code: {response.status_code}")
    


def untar_dataset():
    # tar_path = "/home/project/airflow/dags/python_etl/staging/tolldata.tgz"
    # extract_path = "/home/project/airflow/dags/python_etl/staging"

    tar_path = "./staging/tolldata.tgz"
    extract_path = "./staging"

    # Check if the .tgz file exists
    if not os.path.exists(tar_path):
        raise FileNotFoundError(f"{tar_path} does not exist.")

    # Extract the .tgz file
    with tarfile.open(tar_path, "r:gz") as tar:
        tar.extractall(path=extract_path)
        print(f"Extracted tar file to: {extract_path}")


def extract_data_from_tsv():

    # input_path = "/home/project/airflow/dags/python_etl/staging/tollplaza-data.tsv"
    # output_path = "/home/project/airflow/dags/python_etl/staging/csv_data.csv"
    input_path = "./staging/tollplaza-data.tsv"
    output_path = "./staging/csv_data.csv"

    # Ensure the TSV file exists
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"{input_path} not found")

    print('Read the TSV file (tab-separated)')
    df = pd.read_csv(input_path, sep='\t', header=None)
    print(df.head())
    print(df.columns)
    selected_columns = ["Number of axles", "Tollplaza id", "Tollplaza code"]

    # Check if expected columns exist
    missing_cols = [col for col in selected_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing expected columns: {missing_cols}")

    # Select and save
    df_selected = df[selected_columns]
    df_selected.to_csv(output_path, index=False)
    print(f"Saved extracted TSV data to: {output_path}")
    

if __name__ == "__main__":
    download_dataset()
    untar_dataset()
    extract_data_from_tsv()
    print("Done!")