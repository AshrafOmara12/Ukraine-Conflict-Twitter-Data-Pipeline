# Ukraine-Conflict-Twitter-Data-Pipeline
In this project, I will create a data pipeline to move data from kaggle to gcs then to BigQuery stored as table. The dataset consists around 400 csv files that contains the tweets content related to the Ukraine Conflict from April 2022 till now. The dataset is updated everyday on kaggle dataset [https://www.kaggle.com/datasets/bwandowando/ukraine-russian-crisis-twitter-dataset-1-2-m-rows] with size around 20 GB now,  and I will move the files on daily basis also to gcs. I will create spark job on Dataproc cluster to process the data from gcs to bigquery and make all of the required transformation and partioning before pushing the data to Bigquery.The dataset contains different cases in the text columns that will cleaned and create sentiment analysis on the tweets content to check the sentiment from the begining of the war till now. Then, I will use Google Looker Studio to connect to Bigqquery and build the dashboard. 

# Technologies
- Python the main programmimg language for this project.
- Terraform as infastrucre and a code serivce.
- Google Cloud provider (GCS as the data lake, Bigquery as DWH, and Dataproc as platform for performing ETL operations.
- Prefect orchestration tool.
- Google Looker Studio to build the insights.

# Creating a Project on Google Cloud
- go to [google apis](https://cloud.google.com/) .
- create a new project.
- go to "I AM&Admin" serivce to create a serivce account which will give credentials to use it to access the created serivces:- GCS, Google BigQuery, and Dataproc
- enable APIs of these serivces.
- run this in your cmd export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"
- install google sdk cli to interact with the google serivces.

# Building the Google Serivces using Terraform
- Download and install Terraform.
- check terraform version using terraform -version command. 
- start terraform init
- terraform plan -var="project=<your-gcp-project-id>" add the id of your google project.
- go back to your google accout and check the newly created serivces.

# Prefect as Orchestration tool
- pip install prefect
- open prefect cloud and create a new workplace.
- run '''prefect cloud login''' to auth your prefect account.

# Moving Dataset from Kaggle to GCS
As the dataset already stored as csvs in kaggle, I found that it would be more easily for reproducing the pipeline again to move the data from kaggle to gcs direcly with downloading it to local machine or server (VM instance) that would take money and time. However, I will keep the downloading scenario also if someone will be happy with that.
## Approach 1: If you are interested in kaggle please follow the following steps:
- sign in kaggle and set your account.
- create new juypter notebook inside kaggle Home.
- inside the notebook add the dataset "🇺🇦 Ukraine Conflict Twitter Dataset"
- from add-ons choose Google Cloud services and attach cloud storage to your notebook
- use the script from data pipeline/move_dataset_to_gcs.ipynb and run it in your notebook, the dataset will move directly. it will take no time compared to the other solution.

## Approach 2: 
uncomment the lines inside the flow function and run the script data_pipline.py as mention in the section "How to run this project" Afer adding the required paramters like the name of dataset, target path in which the data will be downloaded.

# Spark Job on Dataproc
for the data processing, I will use Dataproc google cloud service to create a spark cluster and submit the job. 
 I used the python client libraries to create, submit, and delete the cluster after finishing the job. The spark job is mainly creating the required schema, renameing some colums, and drop others and cast the columns datatypes. Then, I read the files from the gcs bucket folder adding some options to clean the data. After that I drop the duplicates to make sure that our records are unique. The main goal of the data to get the sentiment analysis based on the "text" column that contain the tweets content which is cleaned. I used TextBlob library to make this analysis and I install some packages on the top of the nodes of the VMs using this startup.sh file inside the code folder.
 After that I write our final df as a table in bigquery.
 Please note that if you decide to process the whole dataset choose the suitable region that contains sufficient qoutes from I AM serivce then go to qoutes. 
 The job take around 2.5 hourse to move the initial load files. Also, you can move all data and process only 5 files for testing. all you need to choose the path of the initial load that contains 5 files. 
  
# How to run this project
- git clone the repo.
- go to the folder that contains data-pipeline, terraform folders and create virtualen:- python -m venv env then activate your env and install reuirements.txt
- create a google project as explained in the section [Creating a project on Google Cloud], enable serivces apis and create serivce account with permission for the used serivces.
- go to terraform folder and : run terraform init, then terraform plan. and finally terraform apply. it will require the project id please catch it from your google account and if you need to change gcs bucket name, bigquery dataset, or region you can do this in the variables.ttf file. 
- check your GCP and then GCS and bigquery and make sure that the serivces are created correctly.
- move the dataset from kaggle to GCS:- please follow the steps in section "moving data from local machine to gcs" 
- - run  gsutil cp -r code gs://dtc_data_lake_ukraine-tweets-381418/ to push the code to google cloud storage
- My GCS bucket like this:
 ![image](https://user-images.githubusercontent.com/56610966/230196070-c216eac2-225e-4ab3-806f-22dde95740cc.png)
- BigQuery:
 ![image](https://user-images.githubusercontent.com/56610966/230196268-dbb02d52-c6b0-4407-87c7-3c0695c56205.png)
- Dataproc
 ![image](https://user-images.githubusercontent.com/56610966/230197784-965fe371-1695-459d-bf1a-796da7d4c15d.png)

- run the command from file.txt inside the data-pipeline folder.
- check the cleaned, partioned and insights table in your bigquery
- go to google looker studio and create a new report, connect to your bigquery, and start building your dashboard. 

# Dashboard
I have created 2 pages contains some graphs to show the sentiment analysis as a pie chart, and time series graph for the tweets count from the begining till now, also bar graph and google map graphs. Here is the link of the dashboard: [Dashboard]( https://lookerstudio.google.com/reporting/3ab0ab8b-7e45-4c9c-8b38-75e0bd780b2c )
 ![image](https://user-images.githubusercontent.com/56610966/230194759-6d78c339-b30f-47d8-a881-e019f6b8c32a.png)
![image](https://user-images.githubusercontent.com/56610966/230195149-0a695855-b7c8-451c-bbc9-a9810db4a651.png)

 # Enhancement
 - Adding CI/CD test
 - transform the data processing to kafka streaming processing 
 - use the cloud function to deploy the code on google.
 - Analysis the hashtags

 
# Special Thanks
 
 I need to that the zoomcamp team for this amazing journy in which I learned many things and communicate with al ot of amazing people. Also, I need to thank the owner of the data set for his great work of extracting this huge dataset.

