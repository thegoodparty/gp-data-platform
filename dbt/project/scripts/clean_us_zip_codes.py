import logging
import os

import pandas as pd

# Define file paths
local_path = os.path.dirname(os.path.abspath(__file__))
base_path = os.path.join(local_path, "..", "seeds")
input_file = os.path.join(base_path, "US.txt")
output_file = os.path.join(base_path, "us_zip_codes.csv")

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

# Save as CSV
df.to_csv(output_file, index=False, header=True)
logging.info(f"File saved as {output_file}")
