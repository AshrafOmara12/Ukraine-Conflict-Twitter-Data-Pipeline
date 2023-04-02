from google.cloud import dataproc_v1
from google.cloud import storage
import re
from prefect import task, flow
import pandas as pd

@task()
def spark_job_cluster(project_id, region, cluster_name, gcs_bucket, folder, spark_filename):
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
            "worker_config": {"num_instances": 2, "machine_type_uri": "n1-standard-2"},
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

@task()
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

@flow()
def spark_job():
    df_intial = pd.read_csv('gs://dtc_data_lake_ukraine-tweets-381418/code/load.csv')
    if df_intial.loc[:, 'check'].item() ==0:
        print("This is the task for inital load that will run once")
        spark_job_cluster("ukraine-tweets-381418","asia-east1", "ukraine-tweets", "dtc_data_lake_ukraine-tweets-381418", "code" ,"spark_job.py")
    else:
        print("This is the task for delta load that will every time after initial load")
        spark_job_cluster("ukraine-tweets-381418","asia-east1", "ukraine-tweets", "dtc_data_lake_ukraine-tweets-381418", "code" ,"spark_job.py")
        storage_client = storage.Client()
        bucket = storage_client.bucket("dtc_data_lake_ukraine-tweets-381418")    
        blobs = bucket.list_blobs(prefix="delta")
        for blob in blobs:
            file_name = blob.name.split('/')[1]
            move_blob("dtc_data_lake_ukraine-tweets-381418",blob.name,  "dtc_data_lake_ukraine-tweets-381418",f"code/{file_name}")

if __name__ == "__main__":
    spark_job()
