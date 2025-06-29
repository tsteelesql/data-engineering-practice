import requests, os, zipfile
from urllib.parse import urlparse

DIRECTORY_PATH = "Exercises/Exercise-1/downloads"

DOWNLOAD_URIS = [
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
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        zip_file = os.path.join(DIRECTORY_PATH, file_name)
        # Save the content to a local file
        with open(zip_file, "wb") as file:
            file.write(response.content)
        print("Download completed successfully.")
        unzip_file(file_name)

    except requests.RequestException as e:
        print(f"Error downloading {file_name}:\n{e}")
        return


def unzip_file(file_name):
    zip_path = os.path.join(DIRECTORY_PATH, file_name)
    if zipfile.is_zipfile(zip_path):
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(DIRECTORY_PATH)
        print(f"Successfully unzipped file {file_name}")
        delete_zip_file(zip_path)
    else:
        print(f"{file_name} is not a valid zip file.")
    

def main():
    if not os.path.isdir(DIRECTORY_PATH):
        os.mkdir(DIRECTORY_PATH)

    for uri in DOWNLOAD_URIS:
        file_name = get_file_name(uri)
        get_file(uri, file_name)

if __name__ == "__main__":
    main()
