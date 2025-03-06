# TGP to GP Data Migration Scripts

This directory contains the data transformation scripts for migrating data from the legacy `tgp-api` PostgreSQL database to the new `gp-api` PostgreSQL database.

## Overview

The migration process is orchestrated by the `main.sh` script, which serves as the entry point. This script ensures that all necessary steps are executed in the correct order to facilitate a smooth transition of data.

### main.sh

The `main.sh` script is responsible for coordinating the entire migration process. It performs the following tasks:

1. **Data Extraction**: Calls the `table_extract.sh` script to extract data from the `tgp-api` PostgreSQL database.
2. **Data Transformation and Loading**: Calls the `transform_load_executor.sh` script to process and transform the extracted data, then loads it into the `gp-api` PostgreSQL database. **Important Note**: The order of load operations is crucial to ensure that foreign key constraints are satisfied. It is essential to load parent tables before child tables to maintain referential integrity in the `gp-api` database.
3. **Logging and Error Handling**: Logs the migration process and handles any errors that may occur.
