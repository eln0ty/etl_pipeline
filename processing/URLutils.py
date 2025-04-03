# Utilities for URL operations

import csv
import os
import requests
from datetime import datetime

from config import ETL_CONFIG

def url_check(url):
    try:
        response = requests.head(url)
        if response.status_code == 200:
            return True
        else:
            raise Exception(f"URL is not accessible. Status code: {response.status_code}")
    except Exception as e:
        raise Exception(f"Error checking URL: {e}")

def download_csv(csv_url):
    try:
        print(f"Downloading CSV...")
        response = requests.get(csv_url)
        response.raise_for_status()

        content = response.content.decode("utf-8")
        rows = list(csv.reader(content.splitlines()))

        print(f"Successfully downloaded CSV. Rows: {len(rows)}")

        os.makedirs(ETL_CONFIG["data_archive_path"], exist_ok=True)

        # Generate file name with timestamp
        timestamp = datetime.now().strftime(ETL_CONFIG["version_format"])
        file_name = os.path.basename(csv_url)
        archive_path = os.path.join(ETL_CONFIG["data_archive_path"], f"{timestamp}_{file_name}")

        #archive content as CSV format 
        with open(archive_path, "w", encoding="utf-8", newline="") as file:
            writer = csv.writer(file)
            writer.writerows(rows)

        print(f"CSV archived at etl_pipeline/data_archive")

        return content

    except requests.RequestException as e:
        raise Exception(f"Error downloading CSV: {e}")

def generate_table_name(csv_url, prefix_requirement=None):
    table_name = os.path.basename(csv_url).split(".")[0]  # Extract file name without extension
    
    if prefix_requirement and not table_name.lower().startswith(prefix_requirement.lower()):
        raise ValueError(f"The file name does not start with '{prefix_requirement}'.")
    return table_name
