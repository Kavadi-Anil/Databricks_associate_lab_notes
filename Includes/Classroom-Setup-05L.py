# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG dbacademy;
# MAGIC USE SCHEMA IDENTIFIER(DA.schema_name);

# COMMAND ----------

import os

def create_directory_in_user_volume(user_default_volume_path: str, create_folders: list):
    '''
    Creates multiple (or single) directories in the specified volume path.

    Args:
    -------
        user_default_volume_path (str): The base directory path where the folders will be created. 
                                        You can use the default DA.paths.working_dir as the user's volume path.
        create_folders (list): A list of strings representing folder names to be created within the base directory.

    Returns:
    -------
        None: This function does not return any values but prints log information about the created directories.

    Example: 
    -------
    create_directory_in_user_volume(user_default_volume_path=DA.paths.working_dir, create_folders=['customers', 'orders', 'status'])
    '''
    
    print('----------------------------------------------------------------------------------------')
    for folder in create_folders:

        create_folder = f'{user_default_volume_path}/{folder}'

        if not os.path.exists(create_folder):
        # If it doesn't exist, create the directory
            dbutils.fs.mkdirs(create_folder)
            print(f'Creating folder: {create_folder}')

        else:
            print(f"Directory {create_folder} already exists. No action taken.")
        
    print('----------------------------------------------------------------------------------------\n')

create_directory_in_user_volume(user_default_volume_path = DA.paths.working_dir, create_folders = ['csv_demo_files', 'json_demo_files', 'xml_demo_files'])

# COMMAND ----------

import csv

def corrupt_row(input_csv, output_csv):
    """
    Keeps only the first 5 rows and first 3 columns,
    and replaces the first order_id value with 'aaa'.
    """
    with open(input_csv, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.reader(infile, delimiter='|')
        rows = list(reader)

    # Keep only the first 5 rows and first 3 columns
    trimmed_rows = [row[:3] for row in rows[:5]]

    # Replace the first order_id in the first data row (row index 1) with 'aaa'
    if len(trimmed_rows) > 1:
        trimmed_rows[3][0] = 'M_PREM_A,Premium Queen Mattress,#$%^'
        trimmed_rows[3][0] = 'M_PREM_A,Premium Queen Mattress,$100.00'

    with open(output_csv, mode='w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile, delimiter='|')
        writer.writerows(trimmed_rows)

username_cleaned = DA.username.replace('.', '_')
# Example usage
corrupt_row(
    '/Volumes/dbacademy_ecommerce/v01/raw/products-csv/part-00000-tid-1663954264736839188-daf30e86-5967-4173-b9ae-d1481d3506db-2367-1-c000.csv',
    f'/Volumes/dbacademy/ops/{username_cleaned}/csv_demo_files/lab_malformed_data.csv'
)

# COMMAND ----------

# Builds the final lab table for students to use as a resource
query_1 = """
DROP TABLE IF EXISTS 05_lab_solution
"""

query_2 = f"""
CREATE TABLE 05_lab_solution 
USING DELTA AS
SELECT
  *,
  _METADATA.FILE_MODIFICATION_TIME AS file_modification_time,
  _METADATA.FILE_NAME AS source_file, 
  current_timestamp() as ingestion_time
FROM READ_FILES(
        CONCAT('{DA.paths.working_dir}', '/csv_demo_files/lab_malformed_data.csv'),
        FORMAT => "csv",
        SEP => ",",
        HEADER => true,
        SCHEMA => 'item_id STRING, name STRING, price DOUBLE', 
        RESCUEDDATACOLUMN => "_rescued_data"
      )
"""

# Execute the queries
spark.sql(query_1)
spark.sql(query_2)

# COMMAND ----------

# Builds the final lab challenge table for students to use as a resource
query_1 = """
DROP TABLE IF EXISTS 05_lab_challenge_solution
"""

query_2 = f"""
CREATE TABLE 05_lab_challenge_solution 
AS
SELECT
  item_id,
  name,
  price,
  coalesce(price,replace(_rescued_data:price,'$','')) AS price_fixed,
  _rescued_data,
  _metadata.file_modification_time AS file_modification_time,
  _metadata.file_name AS source_file, 
  current_timestamp() as ingestion_time
FROM read_files(
        concat('{DA.paths.working_dir}', '/csv_demo_files/lab_malformed_data.csv'),
        format => "csv",
        sep => ",",
        header => true,
        schema => 'item_id STRING, name STRING, price DOUBLE', 
        rescueddatacolumn => "_rescued_data"
      )
"""


# Execute the queries
r = spark.sql(query_1)
r = spark.sql(query_2)