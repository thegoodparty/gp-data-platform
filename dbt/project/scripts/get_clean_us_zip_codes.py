import logging
import os
import zipfile
from io import BytesIO

import pandas as pd
import requests

# Define file paths
local_path = os.path.dirname(os.path.abspath(__file__))

# Download and extract US.txt file
url = "https://download.geonames.org/export/zip/US.zip"
response = requests.get(url)
with zipfile.ZipFile(BytesIO(response.content)) as zip_ref:
    zip_ref.extract("US.txt", local_path)

input_file = os.path.join(local_path, "US.txt")
output_file = os.path.join(local_path, "..", "seeds", "us_zip_codes.csv")

# Define column names as specified
column_names = [
    "country_code",
    "zip_code",
    "place_name",
    "state_full",
    "state_code",
    "county_province",
    "admin_code2",
    "admin_name3",
    "admin_code3",
    "latitude",
    "longitude",
    "accuracy",
]

columns_to_drop = [
    "country_code",
    "admin_code2",
    "admin_name3",
    "admin_code3",
]

# Read the text file into a pandas DataFrame with specified column names
df = pd.read_csv(input_file, delimiter="\t", header=None, names=column_names)

# Display the first few rows to verify
logging.info(f"Preview of data from {input_file}:")
logging.info(df.head())

# Drop the columns that are not needed
df = df.drop(columns=columns_to_drop)

# Remove rows where place_name is "FPO AA" AND zip_code is either 96860 or 96863, which are duplicate zip codes
df = df[
    ~(
        (df["place_name"] == "FPO AA")
        & ((df["zip_code"] == 96860) | (df["zip_code"] == 96863))
    )
]

# Save as CSV
df.to_csv(output_file, index=False, header=True)
logging.info(f"File saved as {output_file}")

# cleanup
os.remove(input_file)
