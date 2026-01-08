import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
import logging
import time

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
secret_key=os.getenv('AMP_SECRET_KEY')


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
        response = requests.get(url, params=params, auth=(api_key, secret_key),timeout=10)
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

    if response_code != 200:

        dir = "data"
        if not os.path.exists(dir):
            os.mkdir(dir)
        
        data = response.content 
        filepath = f"{dir}/data_{filename}.zip"
        print('Data retrieved successfully.')

        # JSON data files saved to a zip folder 'data.zip'
        with open(filepath, 'wb') as file:
            file.write(data)

        try:
            with open(filepath, 'wb') as file:
                file.write(data)
            print(f"Extract Amplitude data successful at {filepath}")
            logger.info(f"Extract Amplitude data successful {filepath}")
        except Exception as e:
            print(f"Error writing file: {e}")
            logger.error(f"Error writing file: {e}")

        break

    elif response_code > 499 or response_code < 200:
        print(response.reason)
        logger.warning(response.reason)
        time.sleep(10)
        count += 1
        continue
    else:
        print(response.reason)
        logger.error(response.reason)
        break

logger.info("Process Finished")