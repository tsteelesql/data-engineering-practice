import boto3
import os
import gzip
import shutil

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
download_path = f"{DIRECTORY_PATH}/{downloaded_file_name}"


def download_file(bucket_name, object_key, download_path):
    # Initialize an S3 client
    s3 = boto3.client('s3')

    # Download the file
    s3.download_file(bucket_name, object_key, download_path)

    print(f"Downloaded {object_key} from {bucket_name} to {download_path}")


def unzip_gz_file_and_read_first_line(download_path):
    with gzip.open(download_path, 'rt', encoding='utf-8') as file:
        first_line = file.readline()
    
    print(f"Identified URL {first_line}")
    return first_line.strip()



def unzip_gz_file_and_read_each_line(download_path):
    with gzip.open(download_path, 'rt', encoding='utf-8') as file:
        for line in file:
            print(line)


def main():

    if not os.path.isdir(DIRECTORY_PATH):
        os.mkdir(DIRECTORY_PATH)

    download_file(bucket_name, object_key, download_path)
    new_object_key = unzip_gz_file_and_read_first_line(download_path)
    download_file(bucket_name, new_object_key, download_path)
    unzip_gz_file_and_read_each_line(download_path)

if __name__ == "__main__":
    main()
