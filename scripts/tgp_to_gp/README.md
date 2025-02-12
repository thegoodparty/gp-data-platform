# TGP to GP Data Migration Scripts

This directory contains the data transformation scripts for migrating data from the legacy `tgp-api` PostgreSQL database to the new `gp-api` PostgreSQL database. These scripts focus on data transformation rather than direct data movement.

## Overview

These scripts handle the data transformation process as part of the larger API migration from `tgp-api` to `gp-api`. Rather than simply moving data, these scripts perform necessary transformations to ensure the data matches the new schema and requirements of the `gp-api`. Both source and destination are PostgreSQL databases, but they have different schemas and data structures.

## Script Naming Convention

The SQL scripts are named according to the source table names from the `tgp-api` database. Since the new `gp-api` database may use different table names or structures, the script names reflect the original source tables. For example:

- `table_name.sql` - Transforms data from the `table_name` in `tgp-api` into the format required by the corresponding `gp-api` table

## Usage

Each script should be executed in the context of the migration process, ensuring that:
1. The destination tables have been properly created with the correct schema
2. The transformation logic correctly maps the old data structure to the new requirements
