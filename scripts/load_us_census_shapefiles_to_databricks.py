import os
import zipfile
from time import sleep
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")

DBX_SHAPEFILES_VOLUME_PATH = os.environ.get("DBX_SHAPEFILES_VOLUME_PATH")


def download_files(url, save_dir):
    """
    Download multiple files from the internet and save them to a specified directory.

    Parameters:
    urls (list): List of file URLs to download.
    save_dir (str): Directory to save the downloaded files.
    """
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for failed requests

    except requests.exceptions.RequestException as e:
        print(f"Error fetching page: {e}")
        return

    soup = BeautifulSoup(response.text, "html.parser")
    zip_links = []

    # Find all anchor tags
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if href.endswith(".zip"):
            full_link = urljoin(url, href)  # Ensure absolute URL
            zip_links.append(full_link)

    # only keep limited number of states for now
    # zip_links = [link for link in zip_links if "38" in link]

    for url in zip_links:
        filename = os.path.join(save_dir, url.split("/")[-1])

        # Skip if file already exists
        if os.path.exists(filename):
            print(f"Skipping {filename} because it already exists")
            continue

        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()  # Raise an error for bad status codes

            with open(filename, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)

            print(f"Downloaded: {filename}")
        except requests.RequestException as e:
            print(f"Failed to download {url}: {e}")


def unzip_all_files(directory):
    """
    Unzips all .zip files in the given directory.

    Parameters:
    directory (str): Path to the directory containing zip files.
    """
    if not os.path.exists(directory):
        print(f"Directory '{directory}' does not exist.")
        return

    for filename in os.listdir(directory):
        if filename.endswith(".zip"):
            file_path = os.path.join(directory, filename)
            unzipping_path = os.path.join(directory, filename.replace(".zip", ""))
            if os.path.exists(unzipping_path):
                print(f"Skipping {filename} because it already exists")
                continue
            try:
                with zipfile.ZipFile(file_path, "r") as zip_ref:
                    zip_ref.extractall(unzipping_path)
                print(f"Unzipped: {file_path}")
            except zipfile.BadZipFile:
                print(f"Failed to unzip (corrupted file): {file_path}")
            except Exception as e:
                print(f"Error unzipping {file_path}: {e}")


def upload_to_databricks(directory):
    try:
        # Use explicit authentication parameters
        w = WorkspaceClient(
            host=os.environ.get("DATABRICKS_HOST"),
            token=os.environ.get("DATABRICKS_TOKEN"),
        )
    except ValueError as e:
        print(f"Authentication error: {str(e)}")
        return  # Exit the function if authentication fails

    shapefile_directories = [
        os.path.join(directory, d)
        for d in os.listdir(directory)
        if os.path.isdir(os.path.join(directory, d))
    ]
    for shapefile_directory in shapefile_directories:
        volume_path = (
            f"{DBX_SHAPEFILES_VOLUME_PATH}/{shapefile_directory.split('/')[-1]}"
        )
        w.dbfs.mkdirs(volume_path)

        # TODO: convert shapefile to geohash
        print(f"Uploading {shapefile_directory} to {volume_path}")

        # load us_county shapefile manually since it timesout with the w.files.upload() method
        if shapefile_directory.split("/")[-1] == "tl_2024_us_county":
            print(f"Skipping {shapefile_directory} as requested")
            continue

        for file in os.listdir(shapefile_directory):
            print(f"Uploading {file} to {volume_path}")
            max_retries = 3
            for retry in range(max_retries):
                try:
                    with open(f"{shapefile_directory}/{file}", "rb") as f:
                        w.files.upload(
                            file_path=f"{volume_path}/{file}",
                            contents=f,
                            overwrite=True,
                        )
                    break
                except Exception as e:
                    print(f"Error uploading {file}: {e}")
                    sleep(2)
                    if retry < max_retries - 1:
                        print(f"Retrying {file} (attempt {retry + 2}/{max_retries})")
                    else:
                        print(f"Failed to upload {file} after {max_retries} attempts")


shapefiles_directory = "tmp_data/tiger_shapefiles"
tiger_codes = [
    "CD119",
    "SLDU",
    "SLDL",
    "ELSD",
    "SCSD",
    "UNSD",
    "SDADM",
    "PLACE",
    "COUSUB",
    "COUNTY",
    "CONCITY",
    "STATE",
    "COUNTY",
]
base_url = "https://www2.census.gov/geo/tiger/TIGER2024/"
for tiger_code in tiger_codes:
    if tiger_code == "CD119":
        download_files(f"{base_url}/CD/", shapefiles_directory)
    else:
        download_files(f"{base_url}/{tiger_code}/", shapefiles_directory)

unzip_all_files(shapefiles_directory)
upload_to_databricks(shapefiles_directory)
