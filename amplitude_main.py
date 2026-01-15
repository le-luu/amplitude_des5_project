from modules.logging import logging_function
from modules.amp_extract import extract_amplitude_data
from modules.amp_load import list_s3_objects, upload_files_to_s3
import boto3
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

def main():
    load_dotenv()

    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    url = 'https://analytics.eu.amplitude.com/api/2/export'

    API_KEY=os.getenv('AMP_API_KEY')
    SECRET_KEY=os.getenv('AMP_API_SECRET')

    os.makedirs('logs', exist_ok=True)
    log_path = os.path.join(os.getcwd(), "logs")
    logger = logging_function(log_path, timestamp)

    prev_7_days = datetime.now() - timedelta(days=7)
    start_time = prev_7_days.strftime('%Y%m%dT00')
    end_time = datetime.now().strftime('%Y%m%dT00')
    number_of_retries = 5

    AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
    AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
    AWS_BUCKET_NAME = os.getenv('AWS_BUCKET_NAME')

    logger.info("====================Start Extracting Process============================")
    data_path, date_list = extract_amplitude_data(start_time, end_time, url, number_of_retries,API_KEY, SECRET_KEY, timestamp, logger)
    logger.info("====================End Extracting Process==============================")

    # print("Data path in new main is: ", data_path)
    # print("Date list in new main is: ", date_list)

    session = boto3.Session(
    profile_name= 'default'
)

    s3_client = session.client(
        's3'#,
        # aws_access_key_id=AWS_ACCESS_KEY,
        #aws_secret_access_key=AWS_SECRET_KEY
    )

    # s3_client = boto3.client('s3',
    #                          aws_access_key_id=AWS_ACCESS_KEY,
    #                          aws_secret_access_key=AWS_SECRET_KEY)

    prefix_name = "python-import"

    logger.info("====================Start Loading Process============================")
    upload_files_to_s3(s3_client, AWS_BUCKET_NAME, data_path, prefix_name, logger)
    s3_objs, num_objs = list_s3_objects(s3_client, AWS_BUCKET_NAME, prefix_name)
    print("Total objs on s3 now:", num_objs)
    logger.info("====================End Loading Process==============================")

if __name__ == "__main__":
    main()