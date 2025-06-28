import requests, os, zipfile
from urllib.parse import urlparse

DIRECTORY_PATH = 'Exercises\Exercise-1\downloads'

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


def delete_zip_file(zip_path):
        os.remove(zip_path)


def get_file_name(url):
    parsed_url = urlparse(url)
    return os.path.basename(parsed_url.path)


def get_file(url,file_name):
    # Send HTTP GET request
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Save the content to a local file
        with open(f"{DIRECTORY_PATH}\\{file_name}", "wb") as file:
            file.write(response.content)
        print("Download completed successfully.")
        unzip_file(file_name)
    else:
        print(f"Failed to download file. Status code: {response.status_code}")


def unzip_file(file_name):
    zip_path = os.path.join(DIRECTORY_PATH, file_name)
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(DIRECTORY_PATH)
    print(f"Successfully unzipped file {file_name}")
    delete_zip_file(zip_path)


def main():
    if not os.path.isdir(DIRECTORY_PATH):
        os.mkdir(DIRECTORY_PATH)

    for uri in download_uris:
        file_name = get_file_name(uri)
        get_file(uri, file_name)

if __name__ == "__main__":
    main()
