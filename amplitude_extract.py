import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
import logging
import time
from amp_unzip_data import unzip_amplitude_data

# API endpoint is the EU residency server
url = 'https://analytics.eu.amplitude.com/api/2/export'

#Set the filename with timestamp to track the extraction and logging time
filename = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

#Get the start date from 7 days ago to today
prev_7_days = datetime.now() - timedelta(days=7)

params = {
        'start': prev_7_days.strftime('%Y%m%dT00'),
        'end': datetime.now().strftime('%Y%m%dT00')
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

#Extract Amplitude Data function
def extract_amplitude_data():

    #There are 5 retries for failed requests
    number_of_retries = 5
    attempt = 0

    while attempt < number_of_retries:
        try:
            #Send the GET Request
            logger.info(f"Sending request to Amplitude API: {url} with params: {params}")
            response = requests.get(url, params=params, auth=(api_key, secret_key))
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            logger.error(f"Request failed: {e}")
            time.sleep(10)
            attempt += 1
            continue
        except requests.exceptions.Timeout as e:
            print(f"Request timed out: {e}")
            logger.error(f"Request timed out: {e}")
            time.sleep(10)
            attempt += 1
            continue
        except requests.exceptions.ConnectionError as e:
            print(f"Connection error: {e}")
            logger.error(f"Connection error: {e}")
            time.sleep(10)
            attempt += 1
            continue
        
        #Store the response code
        response_code = response.status_code

        #If response is successful:
        if response_code == 200:

            #Create a "data" folder to store the zip files
            dir = "data"
            if not os.path.exists(dir):
                os.mkdir(dir)
            
            #Get the content of the response
            data = response.content 

            #Create a filepath to store the zip files
            filepath = os.path.join(dir, f"data_{filename}.zip")
            print('Data retrieved successfully.')
            logger.info('Data retrieved successfully.')

            # Write zip files into filepath directory
            try:
                with open(filepath, 'wb') as file:
                    file.write(data)
                print(f"Extract Amplitude data successful at {filepath}")
                logger.info(f"Extract Amplitude data successful {filepath}")
            except Exception as e:
                print(f"Error writing file: {e}")
                logger.error(f"Error writing file: {e}")

            #Unzip and decompress the Amplitude data to JSON files
            unzip_amplitude_data(filepath, dir, filename)

            print("Decompressed all .gz files successfully.")
            logger.info("Decompressed all .gz files successfully.")

            break

        elif response_code > 499 or response_code < 200:
            print(response_code,response.reason)
            logger.warning(response_code,response.reason)
            time.sleep(10)
            attempt += 1
            continue
        else:
            print(response_code, response.reason)
            logger.error(response_code,response.reason)
            break

    logger.info("Process Finished")
    print("Process Finished")

def main():
    extract_amplitude_data()

if __name__ == "__main__":
    main()