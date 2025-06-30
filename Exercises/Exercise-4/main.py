import glob
import os
import json
import pandas as pd

base_directory = './Exercises/Exercise-4/data'
output_directory = './Exercises/Exercise-4/output'
search_pattern = '**/*.json'

def find_files(base_directory, search_pattern):
    return glob.glob(os.path.join(base_directory,search_pattern), recursive=True)


def get_csv_file_name(file):
    file_name = os.path.basename(file)
    csv_name = f"{os.path.splitext(file_name)[0]}.csv"
    return os.path.join(output_directory,csv_name)


def read_and_flatten_file(input_file,output_file):
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    df = pd.json_normalize(data)
    df.to_csv(output_file, index=False)


def main():

    try:
        os.makedirs(output_directory, exist_ok=True)

        matching_files = find_files(base_directory, search_pattern)

        for input_file in matching_files:
            print(f"Processing file: {input_file}")
            
            output_file = get_csv_file_name(input_file)
            read_and_flatten_file(input_file,output_file)
        
    except Exception as e:
        print(f"Error Encountered:{e}")

if __name__ == "__main__":
    main()
