import requests
from lxml import html
import os
import pandas as pd

DIRECTORY_PATH = "Exercises/Exercise-2/downloads"

#URL to search and modified date to look for
# Note that modified date in the README did not exist in the webpage anymore, 
# so picked an existing date to search for instead
url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
modified_date = "2024-01-19 15:31"

def find_csv_to_download():
    try:
        response = requests.get(url)
        response.raise_for_status()
        tree = html.fromstring(response.content)
        
        # XPath to find the tr element where any td matches the value
        xpath = f"//table//tr[td[normalize-space() = '{modified_date}']]"
        matching_rows = tree.xpath(xpath)

        if matching_rows:
            print("Found matching row.")
            cells = matching_rows[0].xpath(".//td")
            return cells[0].text_content()
            
        else:
            print("No row contains the specified value.")

    except requests.RequestException as e:
        print(f"Error searching url {url}:\n{e}")
        return


def get_file(file_name):
    # Send HTTP GET request
    try:
        download_url = f"{url}{file_name}"
        response = requests.get(download_url, timeout=10)
        response.raise_for_status()
        download_file = os.path.join(DIRECTORY_PATH, file_name)
        # Save the content to a local file
        with open(download_file, "wb") as file:
            file.write(response.content)
        print("Download completed successfully.")
        return download_file

    except requests.RequestException as e:
        print(f"Error downloading {file_name}:\n{e}")
        return


def load_data_frame(download_file):
    df = pd.read_csv(download_file)
    return df


def find_max_HourlyDryBulbTemperature(df):
    print(df[df['HourlyDryBulbTemperature'] ==  df['HourlyDryBulbTemperature'].max()])


def main():
    if not os.path.isdir(DIRECTORY_PATH):
        os.mkdir(DIRECTORY_PATH)

    file_name = find_csv_to_download()
    download_file = get_file(file_name)
    df = load_data_frame(download_file)
    find_max_HourlyDryBulbTemperature(df)


if __name__ == "__main__":
    main()
