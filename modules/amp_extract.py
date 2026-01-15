import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time
from modules.amp_unzip_data import unzip_amplitude_data
from modules.logging import logging_function

def extract_amplitude_data(start_time, end_time, url, number_of_retries,API_KEY, SECRET_KEY, timestamp, logger):
    params = {
        # 'start': prev_7_days.strftime('%Y%m%dT00'),
        # 'end': datetime.now().strftime('%Y%m%dT00')
        'start': start_time,
        'end': end_time
    }
    #There are 5 retries for failed requests
    attempt = 0

    while attempt < number_of_retries:
        try:
            #Send the GET Request
            logger.info(f"Sending request to Amplitude API: {url} with params: {params}")
            response = requests.get(url, params=params, auth=(API_KEY, SECRET_KEY))
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
            filepath = os.path.join(dir, f"data_{timestamp}.zip")
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
            data_folder_path = unzip_amplitude_data(filepath, dir, timestamp,logger)
            print("Decompressed all .gz files successfully.")
            logger.info("Decompressed all .gz files successfully.")

            zip_file_path = os.path.join(os.getcwd(),filepath)
            os.remove(os.path.join(os.getcwd(),filepath))
            print("Removed .zip file: ",zip_file_path)
            logger.info("Removed .zip file")
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

    logger.info("Extracting Process Finished")
    print("Extracting Process Finished")
    # print(os.getcwd())
    # print(f"All JSON files are ready at: {data_folder_path}")
    data_path = os.path.join(os.getcwd(), data_folder_path)
    #print(f"Start time: {prev_7_days.strftime('%Y%m%d')}, End time: {datetime.now().strftime('%Y%m%d')}")
    #print(f"All JSON files are ready at: {data_path}")
    # min_date= prev_7_days.strftime('%Y%m%d')
    # max_date= datetime.now().strftime('%Y%m%d')

    #create a list to fill in data between min_date and max_date
    #and add postfix time 00 to 23 hours for each date
    date_list = []
    while start_time <= end_time:
        for hour in range(24):
            date_list.append(f"{start_time[:-2]}{hour:02d}")
        min_date_dt = datetime.strptime(start_time, '%Y%m%dT%H')
        min_date_dt += timedelta(days=1)
        start_time = min_date_dt.strftime('%Y%m%dT00')

    return data_path, date_list