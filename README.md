# AMPLITUDE EXTRACTING and LOADING DATA PROJECT

<img src="https://github.com/le-luu/bike_point_project/blob/main/img/TIL%20Logo%20PDF.png" width="300" />

## Objectives
This project is about extracting data from AMPLITUDE API. After extracting data from API, unzip the zip files. Then, decompressed the gz files to get the json files. In each step, handle the error and output the log file.

## Project Structure
```text
├── img
├── .gitignore
├── LICENSE
├── README.md
├── modules
    ├── amp_extract.py
    ├── amp_find_missing_files.py
    ├── amp_load.py
    ├── amp_unzip_data.py
    ├── logging.py
├── amplitude_main.py
├── requirements.txt
```

## Explanation

<img src="https://github.com/le-luu/amplitude_des5_project/blob/main/img/extract_loading_data.png" />

Diagram of the AMPLITUDE Project

### Set up API keys, variables

```python
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
```

Import packages needed to use in the project. Then, set the variables, parameters to prepare for extracting data, including: endpoint url, filename (from the current timestamp), param variable (for start and end timestamp to extract data), load API keys from .env file, setting the logger for logging info in the script.

### Send a GET request
```python
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
```

Send a GET request to the server with the authorization info (API key) and the parameter. In this step, also handle errors could happen during sending the request. They include request failure, timeout, connection error. 

### Write data after receiving response
```python
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
```

After sending the GET request and get the response. Check the response_code; if it's 200 (successfully run), then:
- Create a new folder call "data"
- Create a filepath with the file extension is .zip
- Write the data from response into that zip file
- Handle errors and write info to the log

### Unzip the ZIP file and decompress the .gz files

After getting the ZIP file from the step above, then unzip the ZIP file by using the zipfile package. In the amp_unzip_data.py:
```python
    with zipfile.ZipFile(filepath, 'r') as zip_ref:
        #Store the unzipped files in a folder with the same name as the zip file
        file_unzip_path = os.path.join(dir, f"data_{filename}")
        if not os.path.exists(file_unzip_path):
            os.mkdir(file_unzip_path)

            #Extract all the contents of zip file in file_unzip_path directory
            zip_ref.extractall(file_unzip_path)
            print(f"Unzip Amplitude data successful at {file_unzip_path}")
```

- Create a file_unzip_path to store the data after unzipping
- Create a folder and store all extracted file/folder there
- Handle errors, print out the screen

```python
    for root, _, files in os.walk(day_folder_path):
        for file in files:
            #Check if the files end with .gz
            if file.endswith('.gz'):
                # Process each .gz file to get the full path, filename of those gz files
                gz_path = os.path.join(root, file)
                #Json file name is the filename without .gz extension
                json_file_name = file[:-3]
                #Set the output path for the decompressed json files
                output_path = os.path.join(root, json_file_name)
                try:
                    with gzip.open(gz_path, 'rb') as gz_file, open(output_path, 'wb') as out_file:
                        shutil.copyfileobj(gz_file, out_file)
                except Exception as e:
                    print(f"Error when decompressing file {file}: {e}")
    print("Decompressed all .gz files successfully. All json files are ready at: ",day_folder_path)
```

- After extracting from zip file, access to the account folder using os package
- Iterate to each gz file in the folder
- Create a filepath for json which not including (.gz) at the end
- Use the gzip package to decompress the gz files

 ```python
    for root, _, files in os.walk(day_folder_path):
        for file in files:
            if file.endswith('.gz'):
                os.remove(os.path.join(root, file))
    print("Removed all .gz files successfully.")
```
- Remove the gz files and only keep the JSON files
- Handle errors, print out the screen and write to the log file for each step

### Find missing data when loading data to S3 bucket

After loading data to S3 bucket, compare the files just uploaded to S3 successfully and local files. If any files in the local list not in the s3_list, then store those file names.

```python
def find_missing_files(local_list,s3_list, logger):
    s3_set = set(s3_list)
    missing_in_s3_list = [f for f in local_list if f not in s3_set]
    if len(missing_in_s3_list) > 0:
        print(f"{len(missing_in_s3_list)} file(s) couldn't upload to S3")
        logger.error(f"{len(missing_in_s3_list)} file(s) couldn't upload to S3")
    else:
        print("All files successfully uploaded to S3")
        logger.info("All files successfully uploaded to S3")
    return missing_in_s3_list, len(missing_in_s3_list)
```

Then, return the missing file list and number of missing files in the list. Later, in the main function, if the number of missing files is greater than 0, then extract and load the missing files to S3.

### Handle errors and write to log after running whole program

Print the response code and the reason after sending the request.

## Instructions
- Install Python 3.12 or later version
- Fork and Clone this repo to local computer
- Open the Terminal on Mac or Command Prompt on Windows
- Change directory to bike_point_project
  ```
  cd <directory_after_cloning>/amplitude_des5_project
  ```
- Install and activate the virtual environment. In the terminal, type:
  ```
  python -m venv .venv
  .venv\Scripts\activate
  ```
- Install the packages to run the Python script
  ```
  pip install requirements.txt
  ```
- Create a .env file in the amplitude_des5_project folder
  ```
  AMP_API_KEY = <YOUR AMPLITUDE API KEY>
  AMP_API_SECRET = <YOUR AMPLITUDE SECRET KEY>
  
  AWS_ACCESS_KEY = <YOUR AWS ACCESS KEY>
  AWS_SECRET_KEY = <YOUR AWS SECRET KEY>
  AWS_REGION = <THE REGION of AWS>
  AWS_BUCKET_NAME = <NAME OF S3 BUCKET>
  ```
- Run the Python script:
  ```
  python amplitude_main.py
  ```
