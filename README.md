# Ukraine-Conflict-Twitter-Data-Pipeline
In this project, I will create a data pipeline to move data from kaggle to gcs then to BigQuery stored as table. I will use spark job on Dataproc cluster to process the data from gcs to bigquery and make all of the reuired transformation and partioning before pushing the data to Bigquery. Then, I will use Google Looker Studio to connect to Bigqquery and build the dashboard. 

# Creating a project on Google Cloud
- go to [google apis](https://cloud.google.com/) 
- create a new project.
- go to "I AM&Admin" serivce to create a serivce account which will give credentials to use it to access the created serivces:- GCS, Google BigQuery, and Dataproc
- enable APIs of these serivces.
- run this in your cmd export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"
- install google sdk cli to interact with the google serivces.

# Building the google serivces using terraform
- Download and install Terraform
- check terraform version using terraform -version command. 
- start terraform init
- terraform plan -var="project=<your-gcp-project-id>" add the id of your google project.
- go back to your google accout and check the newly created serivces

# Prefect as orchestration tool.
- pip install prefect
- open prefect cloud and create a new workplace.
- run '''prefect cloud login''' to auth your prefect account.

# moving data from local machine to gcs
As the dataset already stored as csvs in kaggle, I found that it would be more easily for reproducing the pipeline again to move the data from kaggle to gcs direcly with downloading it to local machine or server that would take money and time. However, I will keep the downloading scenario also if someone will be happy with that.
approach1: If you are interested in kaggle please follow the following steps:
1- sign in kaggle and set your account.
2- create new juypter notebook inside kaggle Home.
3- inside the notebook add the dataset "🇺🇦 Ukraine Conflict Twitter Dataset"
4- from addons choose Google Cloud services and attach cloud storage to your notebook
5- use the script from data pipeline/move_dataset_to_gcs.ipynb and run it in your notebook, the dataset will move directly. it will take no time compared to the other solution.

approach2: 
uncomment the lines inside the flow function and run the script data_pipline.py Afer adding the required paramters like the name of dataset, target path in which the data will be downloaded, and if of your bucket to upload the dataset

# spark job on dataproc
for the data processing, I will use Dataproc google cloud service to create a spark cluster and submit the job. I used the python client libraries to create, submit, and delete the cluster after finishing the job.
# How to run this project
- git clone the git repo.
- go to the folder that contains data-pipeline, terraform folders and create virtualen:- python3 -m venv env
- create a google project as explained in the section [Creating a project on Google Cloud], enable serivces apis and create serivce account with permission for the used serivces.
- go to terraform folder and : run terraform init, then terraform plan. and finally terraform apply. it will require the project id please catch it from your google account and if you need to change gcs bucket name, bigquery dataset, or region you can do this in the variables.ttf file. 
- check your GCP and then GCS and bigquery and make sure that the serivces are created correctly.
- move the dataset from kaggle to GCS:- please follow the steps in section "moving data from local machine to gcs"
- run the following command 
  $python data_pipeline.py \
        --initial gs://dtc_data_lake_ukraine-tweets-381418/data/ \  <!-- This is the intial load path of the dataset after uploading it -->
        --output ukraine_tweets_all.ukraine-tweets-analysis \  <!-- This is the output of the bigquery table -->
        --project_id ukraine-tweets-381418 \ <!-- project id -->
        --cluster_region asia-east1 \ <!-- the cluster region that will run the spark job on it -->
        --cluster_name ukraine-tweets \ <!-- the cluster name that will run the spark job on it -->
        --bucket_name dtc_data_lake_ukraine-tweets-381418 \ <!-- the bucket name that contains the data folder -->
        --folder code \ <!-- the folder inside the bucket that contains the spark_job.py, load.csv, and startup.sh files -->
        --file_spark_job spark_job.py \ <!-- the name of the spark job file -->
        --delta gs://dtc_data_lake_ukraine-tweets-381418/delta/ <!-- the path of the delta load files -->
- check the cleaned, partioned and insights table in your bigquery
- go to google looker studio and create a new report, connect to your bigquery, and start building your dashboard.

