import boto3
import os
from dotenv import load_dotenv
from amplitude_extract import extract_amplitude_data
from datetime import datetime, timedelta
import time

load_dotenv()

#Create variables to store all KEY and Bucket name
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
AWS_BUCKET_NAME = os.getenv('AWS_BUCKET_NAME')

# session = boto3.Session(
#     profile_name= 'default'
# )

# s3_client = session.client(
#     's3'#,
#     # aws_access_key_id=AWS_ACCESS_KEY,
#     #aws_secret_access_key=AWS_SECRET_KEY
# )

s3_client = boto3.client('s3',
                         aws_access_key_id=AWS_ACCESS_KEY,
                         aws_secret_access_key=AWS_SECRET_KEY)

#List_s3_objects to list all files in the bucket and the prefix name
def list_s3_objects(AWS_BUCKET_NAME, prefix_name):
    #Create an empty list to store all filename on S3 bucket
    files = []
    paginator = s3_client.get_paginator('list_objects_v2')

    #Iterate to each page to store all filenames
    for page in paginator.paginate(Bucket=AWS_BUCKET_NAME, Prefix=prefix_name):
        if 'Contents' in page:
            files.extend([obj['Key'] for obj in page['Contents']])

            # remove prefix name from file names
            files = [f.replace(prefix_name + '/', '') for f in files]

            #remove empty string in file name
            files = [f for f in files if f]
            files =  [
                    f.split("_", 1)[1].split("#", 1)[0]
                    .replace("-", "")
                    .replace("_", "")
                    for f in files
                ]
            #Keep the left 8 digits (which is 4 digits of the year, 2 digits of the month, 2 digits of the year)
            files = [f[:8] + f[8:].rjust(2, '0') for f in files]
    #Store number of files in a variable 
    num_obj_s3 = len(files)

    return files, num_obj_s3

# upload_files_to_s3 function to upload all files inside the local_data_path to s3 bucket
def upload_files_to_s3(local_data_path):
    # data_path = extract_amplitude_data()
    # print(f"data path from the extract function: {data_path}")

    uploaded_files_num = 0

    #walk through the folder stored json files
    for root, _, files in os.walk(local_data_path):
        #if exist files in the folder
        if len(files) > 0:
            #iterate to each JSON file
            for file in files:
                if file.endswith('.json'):
                    try:
                        #Set the path from the local drive (file_path)
                        #and the destination on s3 bucket to upload files
                        file_path = os.path.join(root, file)
                        aws_file_destination = "python-import/" + file
                        s3_client.upload_file(file_path, AWS_BUCKET_NAME, aws_file_destination)
                        uploaded_files_num += 1
                        #Remove the JSON files in local drive
                        os.remove(file_path)

                    except Exception as e:
                        print(f"Failed to upload {file_path}: {e}")
                        uploaded_files_num -= 1
                else:
                    print("No JSON files found!")
        else:
            print(f"No files found in directory: {root}")

    print(f"All {uploaded_files_num} files uploaded successfully.")
    

def main():

    prev_7_days = datetime.now() - timedelta(days=7)
    start_time = prev_7_days.strftime('%Y%m%dT00')
    end_time = datetime.now().strftime('%Y%m%dT00')

    local_data_path, local_date_list = extract_amplitude_data(start_time, end_time)

    #print(f"data path from the extract function: {local_data_path}")
    #print(f"date list from the extract function: {local_date_list}")

    # for root, _, files in os.walk(local_data_path):
    #     print(f"Files in {root}: {files}")

    upload_files_to_s3(local_data_path)
    s3_objs, num_objs = list_s3_objects(AWS_BUCKET_NAME, "python-import")
    print("S3 objects: ", s3_objs)
    print("Number of files on S3: ", num_objs)

if __name__ == "__main__":
    main()