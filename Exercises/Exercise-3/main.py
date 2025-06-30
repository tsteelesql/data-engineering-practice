import boto3
import os
import gzip
from botocore.exceptions import BotoCoreError, ClientError

"""NOTE: 
Outside this script, the AWS user must be configured in order to use boto3, otherwise a 403 error is returned
User needs created in IAM in AWS, given an access key (which needs configured locally with `aws configure`),
and to have S3 access permissions granted in AWS
"""

DIRECTORY_PATH = "Exercises/Exercise-3/downloads"

# Set the bucket name and the file you want to download
bucket_name = 'commoncrawl'
object_key = 'crawl-data/CC-MAIN-2022-05/wet.paths.gz'
downloaded_file_name = "downloaded_file.gz"
download_path = os.path.join(DIRECTORY_PATH,downloaded_file_name)


def download_file(s3,bucket_name, object_key, download_path):
    # Download the file
    s3.download_file(bucket_name, object_key, download_path)

    print(f"Downloaded {object_key} from {bucket_name} to {download_path}")


def unzip_gz_file_and_read_first_line(download_path):
    with gzip.open(download_path, 'rt', encoding='utf-8') as file:
        first_line = file.readline()
    
    print(f"Identified URL {first_line.strip()}")
    return first_line.strip()



def unzip_gz_file_and_read_each_line(download_path):
    with gzip.open(download_path, 'rt', encoding='utf-8') as file:
        for line in file:
            print(line.strip())


def main():

    os.makedirs(DIRECTORY_PATH, exist_ok=True)

    try:
        # Initialize an S3 client
        s3 = boto3.client('s3')
        
        download_file(s3, bucket_name, object_key, download_path)
        new_object_key = unzip_gz_file_and_read_first_line(download_path)

        if not new_object_key:
            raise ValueError("Empty or invalid first line in the gzip file.")
        
        download_file(s3, bucket_name, new_object_key, download_path)
        unzip_gz_file_and_read_each_line(download_path)
    
    except (BotoCoreError, ClientError) as e:
        print(f"Download failed: {e}")

if __name__ == "__main__":
    main()
