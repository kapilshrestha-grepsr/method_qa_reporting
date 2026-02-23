import os
import pandas as pd
import requests
from tqdm import tqdm
from urllib.parse import urlparse
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_PATH = os.path.join(os.getcwd(), "methodusa_parent")
CURR_FOLDER = os.path.join(BASE_PATH, "current_month_files")
os.makedirs(CURR_FOLDER, exist_ok=True)
SHEET_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSasBh8wRA7G1ESOYyGHelrt8LGk-bNF0OPJHM86MQ_5kfl3M539IZR_eDJ3Iq0a-PXlSBtUhD0GxyG/pub?gid=283416262&single=true&output=csv"

def get_filename_from_response(response, url):
    cd = response.headers.get('content-disposition')
    if cd:
        fname = re.findall('filename="?([^"]+)"?', cd)
        if fname:
            return fname[0]
    path = urlparse(url).path
    filename = os.path.basename(path)
    if not filename or filename in ("download", "latest", "output=csv", "csv"):
        filename = None
    return filename

def download_file(url: str, folder: str):
    try:
        with requests.get(url, stream=True, timeout=60) as response:
            response.raise_for_status()
            filename = get_filename_from_response(response, url)
            if not filename:
                print(f"Could not determine filename for {url}, skipping.")
                return None
            save_path = os.path.join(folder, filename)
            if os.path.exists(save_path):
                print(f"Skipping: file '{filename}' is already present.")
                return filename
            total = int(response.headers.get('content-length', 0))
            with open(save_path, "wb") as f, tqdm(
                desc=f"Downloading {filename}",
                total=total,
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
                leave=False
            ) as bar:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        bar.update(len(chunk))
            print(f"Downloaded: {filename}")
            return filename
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None

def download_section():
    try:
        df = pd.read_csv(SHEET_URL)
    except Exception as e:
        print(f"Failed to read sheet: {e}")
        return

    if "Project Name" not in df.columns or "Latest Dataset" not in df.columns:
        print("Sheet must have 'Project Name' and 'Latest Dataset' columns.")
        return

    tasks = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        for _, row in df.iterrows():
            project_name = str(row["Project Name"]).strip().replace(" ", "_")
            file_url = str(row["Latest Dataset"]).strip()
            if not file_url or file_url.lower() == "nan":
                print(f"No dataset link for {project_name}, skipping.")
                continue
            tasks.append(executor.submit(download_file, file_url, CURR_FOLDER))
        for future in as_completed(tasks):
            future.result()  # To raise exceptions if any

if __name__ == "__main__":
    download_section()