# AMPLITUDE EXTRACTING DATA PROJECT

<img src="https://github.com/le-luu/bike_point_project/blob/main/img/TIL%20Logo%20PDF.png" width="300" />

## Objectives
This project is about extracting data from AMPLITUDE API. After extracting data from API, unzip the zip files. Then, decompressed the gz files to get the json files. In each step, handle the error and output the log file.

## Project Structure
```text
├── img
├── .gitignore
├── LICENSE
├── README.md
├── amplitude_extract.py
├── requirements.txt
```

## Explanation

<img src="https://github.com/le-luu/amplitude_des5_project/blob/main/img/diagram.png" />

Diagram of the AMPLITUDE Project

### Set up API keys, variables

<img src="https://github.com/le-luu/amplitude_des5_project/blob/main/img/import_libraries_set_var.png" />

Import packages needed to use in the project. Then, set the variables, parameters to prepare for extracting data, including: endpoint url, filename (from the current timestamp), param variable (for start and end timestamp to extract data), load API keys from .env file, setting the logger for logging info in the script.

### Send a GET request

<img src="https://github.com/le-luu/amplitude_des5_project/blob/main/img/send_request.png" />

Send a GET request to the server with the authorization info (API key) and the parameter. In this step, also handle errors could happen during sending the request. They include request failure, timeout, connection error. 

### Write data after receiving response

<img src="https://github.com/le-luu/amplitude_des5_project/blob/main/img/extract_store_Data_in_folder.png" />

After sending the GET request and get the response. Check the response_code; if it's 200 (successfully run), then:
- Create a new folder call "data"
- Create a filepath with the file extension is .zip
- Write the data from response into that zip file
- Handle errors and write info to the log

### Unzip the ZIP file and decompress the .gz files

<img src="https://github.com/le-luu/amplitude_des5_project/blob/main/img/unzip_decompressed_files.png" />

After getting the ZIP file from the step above, then unzip the ZIP file by using the zipfile package.
- Create a file_unzip_path to store the data after unzipping
- Create a folder and store all extracted file/folder there
- Handle errors, print out the screen and write to the log file
- After extracting from zip file, access to the account folder using os package
- Iterate to each gz file in the folder
- Create a filepath for json which not including (.gz) at the end
- Use the gzip package to decompress the gz files
- Remove the gz files and only keep the JSON files
- Handle errors, print out the screen and write to the log file for each step

### Handle errors and write to log after running whole program

<img src="https://github.com/le-luu/amplitude_des5_project/blob/main/img/errors_if_cannot_Extract.png" />

Print the response code and the reason after sending the request.

## Instructions
 Install Python 3.12 or later version
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
  ```
- Open the amplitude_extract.py file on Text Editor
  Change the start and end param with format: YYYYMMDDT<hour from 00 to 24>
- Run the Python script:
  ```
  python amplitude_extract.py
  ```
