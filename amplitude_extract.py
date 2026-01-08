import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
import logging


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


log_filename = f"logs/logging_example_{filename}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    filename=log_filename
)
logger = logging.getLogger()

# Send request and get response
number_of_retries = 3
count = 0

while count< number_of_retries:
    response = requests.get(url, params=params, auth=(api_key, secret_key),timeout=10)
    response_code = response.status_code
    