# Ukraine-Conflict-Twitter-Data-Pipeline
In this project, I will create a data pipeline to move data from kaggle to gcs then to BigQuery stored as table. I will use spark job on Dataproc cluster to process the data from gcs to bigquery and make all of the reuired transformation and partioning before pushing the data to Bigquery. Then, I will use Google Looker Studio to connect to Bigqquery and build the dashboard. 

# Creating a project on Google Cloud
1- go to google apis
2- create a new project with a new name
3- after creating the new project, we will go to I AM&Admin serivce to create a serivce account which will give credentials to use it to access the created serivces.
4- install google sdk cli to interact with the google serivces.
5- export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"

# Building the google serivces using terraform
1- make sure that you have already inslaed gcloud cli 
2- refresh the authenication using the command "gcloud auth application-default login"
3- Download and install Terraform
4- give premissions to your serivce account
5- Enable these APIs for your project
6- check terraform version using terraform -version command and add the version the main.tf file 
7- start terraform init
8- terraform plan -var="project=<your-gcp-project-id>" add the id of your google project.
9- go back to your google accout and check the newly created serivces

# Orchersteration Tool
I will use prefect2 as an orchersteration tool. 
1- pip install prefect
2- open prefect cloud and create a new workplace in my case i will create "data-engineering-zoomcamp" 
3- prefect cloud login to auth your prefect account

# moving data from local machine to gcs
As the dataset already stored as csvs in kaggle, I found that it would be more easily for reproducing the pipeline again to move the data from kaggle to gcs direcly with downloading it to local machine or server that would take money and time. However, I will keep the downloading scenario also if someone will be happy with that.

# spark job on dataproc
