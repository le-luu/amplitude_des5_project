import logging
import os
def logging_function(filepath, timestamp):
    '''
    Docstring for logging_function
    
    :param prefix: For the folder of the logs
    :param timestamp: For the name of the logs
    '''
    
    # dir = f'{prefix}_logs'
    # os.makedirs(dir,exist_ok=True)
    filename = timestamp
    # file_path = os.path.join(os.getcwd(),prefix)
    # log_filename = f"{filepath}/{filename}.log"
    log_filename = os.path.join(filepath,f"logging_{filename}.log")


    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        filename=log_filename
    )
    logger = logging.getLogger()

    return logger