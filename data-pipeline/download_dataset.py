import kaggle
import os
import shutil
from tqdm import tqdm
import zipfile
import datetime 
from kaggle.api.kaggle_api_extended import KaggleApi
from google.cloud import storage

# def write_file_name():
#     ''' This function to write yesterday date be be like the name of file yesterday file to download it and upload to gcs as delte load'''
#     tod = datetime.datetime.now()
#     d = datetime.timedelta(days = 3)
#     a = tod - d
#     month_ = "{:02d}".format(a.month)
#     yesterday = f'{a.year}{month_}{a.day}'
#     return yesterday

def download_dataset(name_of_dataset: str, path: str):
    ''' If you decide to download the intial dataset on your local machine you can use this api from kaggle'''
    print('start downloading the dataset')
    kaggle.api.dataset_download_files(f'{name_of_dataset}', path=path, unzip=True)

# def download_recent_files(api):
#     ''' download yesterday file'''
#     print(write_file_name()+'_UkraineCombinedTweetsDeduped.csv.gzip')
#     kaggle.api.dataset_download_file(dataset="bwandowando/ukraine-russian-crisis-twitter-dataset-1-2-m-rows",
#     file_name=write_file_name()+'_UkraineCombinedTweetsDeduped.csv.gzip')
#     # print('pass')


def unzip_dataset(path_to_zip_file, directory_to_extract_to):
    ''' extracting the dataset zip folder'''
    with zipfile.ZipFile(path_to_zip_file, 'r') as zip_ref:
        zip_ref.extractall(directory_to_extract_to)

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
        # exit()
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




if __name__ == "__main__":
    api = KaggleApi()
    api.authenticate()
    # download_recent_files(api)
    target_dataset = input('please enter your dataset: ')
    target_path = input('please enter target path: ')
    download_dataset(name_of_dataset=target_dataset, path=target_path)
    if len(os.listdir('combined_files')) == 0:
        for directory in ['archive', 'archive/UkraineWar/UkraineWar']:
            collect_files_in_one_directory(directory, new_destination= 'combined_files')
    else:
        rename_files('combined_files')
    list_files_in_gcs("dtc_data_lake_ukraine-tweets-381418")
    upload_blob("dtc_data_lake_ukraine-tweets-381418", "combined_files")