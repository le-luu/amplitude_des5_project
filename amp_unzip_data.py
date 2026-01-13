import os
import zipfile 
import gzip
import shutil

#Unzip and decompress the Amplitude data to JSON files
def unzip_amplitude_data(filepath, dir,filename):

    #Unzip the Amplitude zip files
    with zipfile.ZipFile(filepath, 'r') as zip_ref:
        #Store the unzipped files in a folder with the same name as the zip file
        file_unzip_path = os.path.join(dir, f"data_{filename}")
        if not os.path.exists(file_unzip_path):
            os.mkdir(file_unzip_path)

            #Extract all the contents of zip file in file_unzip_path directory
            zip_ref.extractall(file_unzip_path)
            print(f"Unzip Amplitude data successful at {file_unzip_path}")
            #logger.info(f"Unzip Amplitude data successful at {file_unzip_path}") 

    #Access to the Account folder after unzipping the zip files
    #iterate to all folders and check if the folder is digit
    day_folder = next(f for f in os.listdir(file_unzip_path) if f.isdigit())
    #Join that folder with the current directory for day_folder_path
    day_folder_path = os.path.join(file_unzip_path, day_folder)

    #Iterate through all files in the day_folder_path
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
    #logger.info("Decompressed all .gz files successfully.")
    
    #Remove all .gz files after decompression
    for root, _, files in os.walk(day_folder_path):
        for file in files:
            if file.endswith('.gz'):
                os.remove(os.path.join(root, file))
    print("Removed all .gz files successfully.")
    return day_folder_path
    #logger.info("Removed all .gz files successfully.")