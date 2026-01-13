import boto3
import os
from dotenv import load_dotenv
from amplitude_extract import extract_amplitude_data

load_dotenv()

AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
AWS_BUCKET_NAME = os.getenv('AWS_BUCKET_NAME')

session = boto3.Session(
    profile_name= 'default'
)

s3_client = session.client(
    's3'#,
    # aws_access_key_id=AWS_ACCESS_KEY,
    #aws_secret_access_key=AWS_SECRET_KEY
)

def list_s3_objects(AWS_BUCKET_NAME, prefix_name):
    files = []
    paginator = s3_client.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=AWS_BUCKET_NAME, Prefix=prefix_name):
        if 'Contents' in page:
            files.extend([obj['Key'] for obj in page['Contents']])
            # remove prefix from file names
            files = [f.replace(prefix_name + '/', '') for f in files]
            #remove empty string in file name
            files = [f for f in files if f]
            files =  [
                    f.split("_", 1)[1].split("#", 1)[0]
                    .replace("-", "")
                    .replace("_", "")
                    for f in files
                ]


    return files

def upload_files_to_s3(local_data_path):
    # data_path = extract_amplitude_data()
    # print(f"data path from the extract function: {data_path}")

    uploaded_files_num = 0
    for root, _, files in os.walk(local_data_path):
        if len(files) > 0:
            for file in files:
                if file.endswith('.json'):
                    try:
                        file_path = os.path.join(root, file)
                        aws_file_destination = "python-import/" + file
                        s3_client.upload_file(file_path, AWS_BUCKET_NAME, aws_file_destination)
                        uploaded_files_num += 1
                        os.remove(file_path)
                        #print(f"Uploaded {file_path} to s3://{AWS_BUCKET_NAME}/python-import/{file}")
                    except Exception as e:
                        print(f"Failed to upload {file_path}: {e}")
                        uploaded_files_num -= 1
                else:
                    print("No JSON files found!")
        else:
            print(f"No files found in directory: {root}")


    print(f"All {uploaded_files_num} files uploaded successfully.")

def main():

    local_data_path, local_date_list = extract_amplitude_data()
    print(f"data path from the extract function: {local_data_path}")
    print(f"date list from the extract function: {local_date_list}")

    # for root, _, files in os.walk(local_data_path):
    #     print(f"Files in {root}: {files}")

    upload_files_to_s3(local_data_path)

    print("List files in S3:",list_s3_objects(AWS_BUCKET_NAME, "python-import"))

if __name__ == "__main__":
    main()