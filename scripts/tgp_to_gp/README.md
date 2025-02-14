# TGP to GP Data Migration Scripts

This directory contains the data transformation scripts for migrating data from the legacy `tgp-api` PostgreSQL database to the new `gp-api` PostgreSQL database.

## Overview

The migration process is orchestrated by the `main.sh` script, which serves as the entry point. This script ensures that all necessary steps are executed in the correct order to facilitate a smooth transition of data.

### main.sh

The `main.sh` script is responsible for coordinating the entire migration process. It performs the following tasks:

1. **Data Export**: Extracts data from the `tgp-api` PostgreSQL database.
2. **Data Transformation**: Processes and transforms the exported data to fit the schema of the `gp-api` database.
3. **Data Import**: Loads the transformed data into the `gp-api` PostgreSQL database.
4. **Logging and Error Handling**: Logs the migration process and handles any errors that may occur.
