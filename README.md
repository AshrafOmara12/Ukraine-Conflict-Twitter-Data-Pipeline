# Ukraine-Conflict-Twitter-Data-Pipeline
In this project, I will create a data pipeline to move data from kaggle to gcs then to BigQuery stored as table. I will use spark job on Dataproc cluster to process the data from gcs to bigquery and make all of the reuired transformation and partioning before pushing the data to Bigquery. Then, I will use Google Looker Studio to connect to Bigqquery and build the dashboard. 

# Creating a project on Google Cloud
- go to google apis
- create a new project with a new name
- after creating the new project, we will go to I AM&Admin serivce to create a serivce account which will give credentials to use it to access the created serivces.
- install google sdk cli to interact with the google serivces.
- export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"

# Building the google serivces using terraform
- make sure that you have already inslaed gcloud cli 
- refresh the authenication using the command "gcloud auth application-default login"
- Download and install Terraform
- give premissions to your serivce account
- Enable these APIs for your project
- check terraform version using terraform -version command and add the version the main.tf file 
- start terraform init
- terraform plan -var="project=<your-gcp-project-id>" add the id of your google project.
- go back to your google accout and check the newly created serivces

# Orchersteration Tool
I will use prefect2 as an orchersteration tool. 
1- pip install prefect
2- open prefect cloud and create a new workplace in my case i will create "data-engineering-zoomcamp" 
3- prefect cloud login to auth your prefect account

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
First of all create virtual env and install requirements. pip install -r requirements.txt
Then follow these steps:
1- create a google project
2- create serive account and make it as owner
3- run this in cli export GOOGLE_APPLICATION_CREDENTIALS="<path/to/authkeys>.json" please replace the path of json file
4- using the terrafom files inside:- 
please run terraform init, then terraform plan. and finally terraform apply
5- check your GCP and then GCS and bigquery 
6- After creating the GCS and bigquery, we will move the dataset from kaggle to GCS:-
Please follow the steps in section " moving data from local machine to gcs"
7- Then, run the data_pipeline.py script
8- The data will be cleaned and partioned to moved to Bigquery
9- open google looker studio
