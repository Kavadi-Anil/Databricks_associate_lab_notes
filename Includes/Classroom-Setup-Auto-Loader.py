# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG dbacademy;
# MAGIC USE SCHEMA IDENTIFIER(DA.schema_name);
# MAGIC
# MAGIC DROP TABLE IF EXISTS python_csv_autoloader;
# MAGIC DROP TABLE IF EXISTS sql_csv_autoloader;

# COMMAND ----------

spark.sql(f'DROP VOLUME IF EXISTS dbacademy.{DA.schema_name}.auto_loader_staging_files')
spark.sql(f'DROP VOLUME IF EXISTS dbacademy.{DA.schema_name}.csv_files_autoloader_source')
spark.sql(f'DROP VOLUME IF EXISTS dbacademy.{DA.schema_name}.auto_loader_files')

create_volume(in_catalog = 'dbacademy', in_schema = f'{DA.schema_name}', volume_name = 'auto_loader_staging_files')
create_volume(in_catalog = 'dbacademy', in_schema = f'{DA.schema_name}', volume_name = 'csv_files_autoloader_source')

# COMMAND ----------

## Copy one file to the cloud storage to ingest
copy_files(copy_from = '/Volumes/dbacademy_ecommerce/v01/raw/sales-csv/', copy_to = f"/Volumes/dbacademy/{DA.schema_name}/csv_files_autoloader_source/", n=1)


## Copy multiple files to the cloud storage staging area to copy into the above volume during the demonstration
copy_files(copy_from = '/Volumes/dbacademy_ecommerce/v01/raw/sales-csv/', copy_to = f"/Volumes/dbacademy/{DA.schema_name}/auto_loader_staging_files/", n=3)