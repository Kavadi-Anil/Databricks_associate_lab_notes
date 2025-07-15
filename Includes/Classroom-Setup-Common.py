# Databricks notebook source
# MAGIC %run ../../Includes/_common

# COMMAND ----------

DA = DBAcademyHelper()
DA.init()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE DA VARIABLE USING SQL FOR USER INFORMATION FROM THE META TABLE FOR SQL SCRIPTS
# MAGIC
# MAGIC -- Create a temp view storing information from the obs table.
# MAGIC CREATE OR REPLACE TEMP VIEW user_info AS
# MAGIC SELECT map_from_arrays(collect_list(replace(key,'.','_')), collect_list(value))
# MAGIC FROM dbacademy.ops.meta;
# MAGIC
# MAGIC -- Create SQL dictionary var (map)
# MAGIC DECLARE OR REPLACE DA MAP<STRING,STRING>;
# MAGIC
# MAGIC -- Set the temp view in the DA variable
# MAGIC SET VAR DA = (SELECT * FROM user_info);
# MAGIC
# MAGIC DROP VIEW IF EXISTS user_info;

# COMMAND ----------

def create_volume(in_catalog: str, in_schema: str, volume_name: str):
    '''
    Create a volume in the specified catalog.schema.
    '''
    print(f'Creating volume: {in_catalog}.{in_schema}.{volume_name} if not exists.\n')
    r = spark.sql(f'CREATE VOLUME IF NOT EXISTS {in_catalog}.{in_schema}.{volume_name}')

# COMMAND ----------

def delete_source_files(source_files: str):
    """
    Deletes all files in the specified source volume.

    This function iterates through all the files in the given volume,
    deletes them, and prints the name of each file being deleted.

    Parameters:
    - source_files : str
        The path to the volume containing the files to delete. 
        Use the {DA.paths.working_dir} to dynamically navigate to the user's volume location in dbacademy/ops/vocareumlab@name:
            Example: DA.paths.working_dir = /Volumes/dbacademy/ops/vocareumlab@name

    Returns:
    - None. This function does not return any value. It performs file deletion and prints all files that it deletes. If no files are found it prints in the output.

    Example:
    - delete_source_files(f'{DA.paths.working_dir}/pii/stream_source/user_reg')
    """

    import os

    print(f'\nSearching for files in {source_files} volume to delete prior to creating files...')
    if os.path.exists(source_files):
        list_of_files = sorted(os.listdir(source_files))
    else:
        list_of_files = None

    if not list_of_files:  # Checks if the list is empty.
        print(f"No files found in {source_files}.\n")
    else:
        for file in list_of_files:
            file_to_delete = source_files + file
            print(f'Deleting file: {file_to_delete}')
            dbutils.fs.rm(file_to_delete)

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

def copy_files(copy_from: str, copy_to: str, n: int, sleep=2):
    '''
    Copy files from one location to another destination's volume.

    This method performs the following tasks:
      1. Lists files in the source directory and sorts them. Sorted to keep them in the same order when copying for consistency.
      2. Verifies that the source directory has at least `n` files.
      3. Copies files from the source to the destination, skipping files already present at the destination.
      4. Pauses for `sleep` seconds after copying each file.
      5. Stops after copying `n` files or if all files are processed.
      6. Will print information on the files copied.
    
    Parameters
    - copy_from (str): The source directory where files are to be copied from.
    - copy_to (str): The destination directory where files will be copied to.
    - n (int): The number of files to copy from the source. If n is larger than total files, an error is returned.
    - sleep (int, optional): The number of seconds to pause after copying each file. Default is 2 seconds.

    Returns:
    - None: Prints information to the log on what files it's loading. If the file exists, it skips that file.

    Example:
    - copy_files(copy_from='/Volumes/gym_data/v01/user-reg', 
           copy_to=f'{DA.paths.working_dir}/pii/stream_source/user_reg',
           n=1)
    '''
    import os
    import time

    print(f"\n----------------Loading files to user's volume: '{copy_to}'----------------")

    ## List all files in the copy_from volume and sort the list
    list_of_files_to_copy = sorted(os.listdir(copy_from))
    total_files_in_copy_location = len(list_of_files_to_copy)

    ## Get a list of files in the source
    list_of_files_in_source = os.listdir(copy_to)

    assert total_files_in_copy_location >= n, f"The source location contains only {total_files_in_copy_location} files, but you specified {n}  files to copy. Please specify a number less than or equal to the total number of files available."

    ## Looping counter
    counter = 1

    ## Load files if not found in the co
    for file in list_of_files_to_copy:
      if file.startswith('_'):
        pass
      else:
        ## If the file is found in the source, skip it with a note. Otherwise, copy file.
        if file in list_of_files_in_source:
          print(f'File number {counter} - {file} is already in the source volume "{copy_to}". Skipping file.')
        else:
          file_to_copy = f'{copy_from}{file}'
          copy_file_to = f'{copy_to}{file}'
          print(f'File number {counter} - Copying file {file_to_copy} --> {copy_file_to}.')
          dbutils.fs.cp(file_to_copy, copy_file_to , recurse = True)
          
          ## Sleep after load
          time.sleep(sleep) 

        ## Stop after n number of loops based on argument.
        if counter == n:
          break
        else:
          counter = counter + 1

# COMMAND ----------

def drop_tables(in_catalog: str, in_schema: list, dry_run: bool = False):
    """
    Drops all tables and views in the specified schema within a given catalog.

    Args:
        in_catalog (str): The catalog name (e.g., 'dbacademy_peter').
        in_schema (str): The schema name (e.g., 'default').
        dry_run (bool): If True, only prints tables that would be dropped without actually dropping them.

    Returns:
        list: Fully qualified names of the tables that were dropped (or would be dropped in dry-run mode).

    Example:
    >>> drop_tables(in_catalog='dbacademy_peter', in_schema='1_bronze_db')
    """
    # Check if catalog exists
    catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
    if in_catalog not in catalogs:
        raise ValueError(f"Catalog '{in_catalog}' does not exist.")

    ## Delete tables and views in schema
    for schema in in_schema:
        
        # Check if schema exists in the catalog
        full_schema = f"{in_catalog}.{schema}"
        if not spark.catalog.databaseExists(full_schema):
            raise ValueError(f"Schema '{schema}' does not exist in catalog '{in_catalog}'.")
        
        print(f"\n{'Previewing' if dry_run else 'Dropping'} all tables in {in_catalog}.{schema}:")

        # Get all tables in the schema
        tables = spark.sql(f"SHOW TABLES IN {in_catalog}.{schema}").collect()

        if not tables:
            print(f"No tables found in schema {in_catalog}.{schema}. Nothing to drop.")
        else:
            table_names = [f"{in_catalog}.{schema}.{t.tableName}" for t in tables]

            for table_full_name in table_names:
                if dry_run:
                    print(f"Would drop: {table_full_name}")
                else:
                    try:
                        spark.sql(f"DROP TABLE IF EXISTS {table_full_name}")
                        print(f"Dropped TABLE: {table_full_name}")
                    except:
                        spark.sql(f"DROP VIEW IF EXISTS {table_full_name}")
                        print(f"Dropped VIEW: {table_full_name}")