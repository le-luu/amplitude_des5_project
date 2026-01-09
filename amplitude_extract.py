import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
import logging
import time
import zipfile 
import gzip
import shutil

# API endpoint is the EU residency server
url = 'https://analytics.eu.amplitude.com/api/2/export'
filename = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

params = {
        'start': '20251208T00',
        'end': '20260108T09'
}

#Load API Keys
load_dotenv()
api_key=os.getenv('AMP_API_KEY')
secret_key=os.getenv('AMP_API_SECRET')

# Set up Logging
logs_dir = "logs"
if not os.path.exists(logs_dir):
    os.mkdir(logs_dir)

log_filename = f"logs/logging_data_{filename}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    filename=log_filename
)
logger = logging.getLogger()

# Send request and get response
number_of_retries = 5
count = 0

while count< number_of_retries:
    try:
        logger.info(f"Sending request to Amplitude API: {url} with params: {params}")
        response = requests.get(url, params=params, auth=(api_key, secret_key))
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        logger.error(f"Request failed: {e}")
        time.sleep(10)
        count += 1
        continue
    except requests.exceptions.Timeout as e:
        print(f"Request timed out: {e}")
        logger.error(f"Request timed out: {e}")
        time.sleep(10)
        count += 1
        continue
    except requests.exceptions.ConnectionError as e:
        print(f"Connection error: {e}")
        logger.error(f"Connection error: {e}")
        time.sleep(10)
        count += 1
        continue

    response_code = response.status_code

    if response_code == 200:

        dir = "data"
        if not os.path.exists(dir):
            os.mkdir(dir)
        
        data = response.content 
        filepath = os.path.join(dir, f"data_{filename}.zip")
        print('Data retrieved successfully.')
        logger.info('Data retrieved successfully.')

        # Write zip files into directory
        try:
            with open(filepath, 'wb') as file:
                file.write(data)
            print(f"Extract Amplitude data successful at {filepath}")
            logger.info(f"Extract Amplitude data successful {filepath}")
        except Exception as e:
            print(f"Error writing file: {e}")
            logger.error(f"Error writing file: {e}")

        with zipfile.ZipFile(filepath, 'r') as zip_ref:
            file_unzip_path = os.path.join(dir, f"data_{filename}")
            if not os.path.exists(file_unzip_path):
                os.mkdir(file_unzip_path)
                zip_ref.extractall(file_unzip_path)
                print(f"Unzip Amplitude data successful at {file_unzip_path}")
                logger.info(f"Unzip Amplitude data successful at {file_unzip_path}")

        #Access to the Account folder after unzipping the zip files
        day_folder = next(f for f in os.listdir(file_unzip_path) if f.isdigit())
        day_folder_path = os.path.join(file_unzip_path, day_folder)

        for root, _, files in os.walk(day_folder_path):
            for file in files:
                if file.endswith('.gz'):
                    # Process each .gz file
                    gz_path = os.path.join(root, file)
                    json_file_name = file[:-3]
                    output_path = os.path.join(root, json_file_name)
                    try:
                        with gzip.open(gz_path, 'rb') as gz_file, open(output_path, 'wb') as out_file:
                            shutil.copyfileobj(gz_file, out_file)
                    except Exception as e:
                        print(f"Error when decompressing file {file}: {e}")
                        logger.error(f"Error when decompressing file {file}: {e}")
        print("Decompressed all .gz files successfully. All json files are ready at: ",day_folder_path)
        logger.info("Decompressed all .gz files successfully.")
        
        for root, _, files in os.walk(day_folder_path):
            for file in files:
                if file.endswith('.gz'):
                    os.remove(os.path.join(root, file))
        print("Removed all .gz files successfully.")
        logger.info("Removed all .gz files successfully.")
        break

    elif response_code > 499 or response_code < 200:
        print(response_code,response.reason)
        logger.warning(response.reason)
        time.sleep(10)
        count += 1
        continue
    else:
        print(response_code, response.reason)
        logger.error(response.reason)
        break

logger.info("Process Finished")