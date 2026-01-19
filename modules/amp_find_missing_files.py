

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