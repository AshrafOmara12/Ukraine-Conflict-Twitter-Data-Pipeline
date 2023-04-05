from google.cloud import dataproc_v1
from google.cloud import storage
import re
from prefect import task, flow
import pandas as pd
import kaggle
import os
import shutil
from tqdm import tqdm
import zipfile
from kaggle.api.kaggle_api_extended import KaggleApi
import argparse
from datetime import timedelta

parser = argparse.ArgumentParser(description='My PySpark job')
parser.add_argument('--initial', type=str, help='Input files path')
parser.add_argument('--output', type=str, help='Output path')
parser.add_argument('--project_id', type=str, help='Output path')
parser.add_argument('--cluster_region', type=str, help='Output path')
parser.add_argument('--cluster_name', type=str, help='Output path')
parser.add_argument('--bucket_name', type=str, help='Output path')
parser.add_argument('--folder', type=str, help='Output path')
parser.add_argument('--file_spark_job', type=str, help='Output path')
parser.add_argument('--delta', type=str, help='Output path')
args = parser.parse_args()

@task()
def download_dataset(name_of_dataset: str, path: str):
    ''' If you decide to download the intial dataset on your local machine you can use this api from kaggle'''
    print('start downloading the dataset')
    kaggle.api.dataset_download_files(f'{name_of_dataset}', path=path)

@task()
def unzip_dataset(path_to_zip_file, directory_to_extract_to):
    ''' extracting the dataset zip folder'''
    with zipfile.ZipFile(path_to_zip_file, 'r') as zip_ref:
        zip_ref.extractall(directory_to_extract_to)

@task()
def collect_files_in_one_directory(files_directory: str, new_destination: str):
    ''' collecting all files of dataset in one folder '''
    if not os.path.isdir(new_destination):
        print('creating folder to collect all dataset files in one folder')
        os.mkdir(new_destination)
    i = 0
    for file in tqdm(os.listdir(files_directory)):
        if file.endswith('.csv.gzip') or file.endswith('.csv.gz'):
            shutil.copy(os.path.join(files_directory,file), new_destination)
            i += 1
            print(f'copy file number {i} to {new_destination}')
    return new_destination

@task()
def rename_files(folder_name: str):
    ''' rename files'''
    for file in os.listdir(folder_name):
        if file.endswith('.csv.gzip'):
            old_name = os.path.join(folder_name, file)
            new_name = os.path.join(folder_name, file.replace('.gzip', '.gz'))
            os.rename(old_name, new_name)

def list_files_in_gcs(bucket_name: str):
    """Lists all the blobs in the bucket."""
    # bucket_name = "your-bucket-name"
    storage_client = storage.Client()

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name)

    # Note: The call returns a response only when the iterator is consumed.
    uploaded_files = []
    for blob in blobs:
        uploaded_files.append(blob.name)
    return uploaded_files


def upload_blob(bucket_name, folder_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    for file in os.listdir(folder_name):
        check_name = file.split('.gz')[0]
        if storage.Blob(bucket=bucket, name=check_name+'.gzip').exists(storage_client):
            print('exists')
            continue
        else:
            print(file)
            blob = bucket.blob(check_name+'.gzip')

            generation_match_precondition = 0

            blob.upload_from_filename(os.path.join(folder_name, file), if_generation_match=generation_match_precondition)

            print(
                f"File {file} uploaded to {check_name+'.gzip'}."
            )

@task(name="dataproc spark job", description="In this task, I will create a spark cluster, submit a job and then deleting it", log_prints=True, timeout_seconds=7200)
def spark_job_cluster(project_id, region, cluster_name, gcs_bucket, folder, spark_filename):
    ''' The default max time out for prefect task is 15 mins or 900 sec. I add 3600 sec as 1 hour for this task'''
    # Create the cluster client.
    cluster_client = dataproc_v1.ClusterControllerClient(
        client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
    )

    # Create the cluster config.
    cluster = {
        "project_id": project_id,
        "cluster_name": cluster_name,
        "config": {
            "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-2"},
            "worker_config": {"num_instances": 3, "machine_type_uri": "n1-standard-2"},
            "initialization_actions": [ {"executable_file": f"gs://{gcs_bucket}/code/startup.sh"}]
        },
    }

    # Create the cluster.
    operation = cluster_client.create_cluster(
        request={"project_id": project_id, "region": region, "cluster": cluster}
    )
    result = operation.result()

    print("Cluster created successfully: {}".format(result.cluster_name))
        # Create the job client.
    job_client = dataproc_v1.JobControllerClient(
        client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
    )

    # Create the job config.
    job = {
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {"main_python_file_uri": "gs://{}/{}/{}".format(gcs_bucket,folder ,spark_filename),
                        "args" : [args.initial, args.output],
                        "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"]},
    }

    operation = job_client.submit_job_as_operation(
        request={"project_id": project_id, "region": region, "job": job}
    )
    response = operation.result()

    # Dataproc job output is saved to the Cloud Storage bucket
    # allocated to the job. Use regex to obtain the bucket and blob info.
    matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)

    output = (
        storage.Client()
        .get_bucket(matches.group(1))
        .blob(f"{matches.group(2)}.000000000")
        .download_as_string()
    )

    print(f"Job finished successfully: {output}\r\n")
    # Delete the cluster once the job has terminated.
    operation = cluster_client.delete_cluster(
        request={
            "project_id": project_id,
            "region": region,
            "cluster_name": cluster_name,
        }
    )
    operation.result()

    print("Cluster {} successfully deleted.".format(cluster_name))

@task(name='move files', description= 'after running the job on delta load, we will move them to inial files directory')
def move_blob(bucket_name, blob_name, destination_bucket_name, destination_blob_name,):
    """Moves a blob from one bucket to another with a new name."""
    storage_client = storage.Client()
    source_bucket = storage_client.bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)

    destination_generation_match_precondition = 0

    blob_copy = source_bucket.copy_blob(
        source_blob, destination_bucket, destination_blob_name, if_generation_match=destination_generation_match_precondition,
    )
    source_bucket.delete_blob(blob_name)

    print(
        "Blob {} in bucket {} moved to blob {} in bucket {}.".format(
            source_blob.name,
            source_bucket.name,
            blob_copy.name,
            destination_bucket.name,
        )
    )

@flow(name="data pipeline flow")
def data_pipeline():
    # api = KaggleApi()
    # api.authenticate()
    # download_dataset(name_of_dataset="data", path="data")
    # if len(os.listdir('combined_files')) == 0:
    #     for directory in ['archive', 'archive/UkraineWar/UkraineWar']:
    #         collect_files_in_one_directory(directory, new_destination= 'combined_files')
    # else:
    #     rename_files('combined_files')
    # list_files_in_gcs("dtc_data_lake_ukraine-tweets-381418")
    # upload_blob("dtc_data_lake_ukraine-tweets-381418", "combined_files")
    df_intial = pd.read_csv(f'gs://dtc_data_lake_ukraine-tweets-381418/code/load.csv')
    if df_intial.loc[:, 'check'].item() ==0:
        print("This is the task for inital load that will run once")
        spark_job_cluster(args.project_id,args.cluster_region, args.cluster_name, args.bucket_name, args.folder ,args.file_spark_job)
    else:
        print("This is the task for delta load that will every time after initial load")
        spark_job_cluster(args.project_id,args.cluster_region, args.cluster_name, args.bucket_name, args.folder ,args.file_spark_job)
        storage_client = storage.Client()
        bucket = storage_client.bucket(args.bucket_name)    
        blobs = bucket.list_blobs(prefix="delta")
        for blob in blobs:
            file_name = blob.name.split('/')[1]
            move_blob(args.bucket_name,blob.name,  args.bucket_name,f"code/{file_name}")

if __name__ == "__main__":
    data_pipeline()
