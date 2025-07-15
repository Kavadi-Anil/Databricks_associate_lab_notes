-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Appendix - Python Auto Loader
-- MAGIC ### Extra material, not part of a live teach.
-- MAGIC
-- MAGIC In this demonstration we will introduce running Auto Loader in Python for incremental ingestion. In this example you will be execute Auto Loader manually to incrementally ingest the data.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## REQUIRED - SELECT CLASSIC COMPUTE
-- MAGIC
-- MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default.
-- MAGIC
-- MAGIC Follow these steps to select the classic compute cluster:
-- MAGIC
-- MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**.
-- MAGIC
-- MAGIC 1. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
-- MAGIC
-- MAGIC   - In the drop-down, select **More**.
-- MAGIC
-- MAGIC   - In the **Attach to an existing compute resource** pop-up, select the first drop-down. You will see a unique cluster name in that drop-down. Please select that cluster.
-- MAGIC
-- MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
-- MAGIC
-- MAGIC 1. Right-click on **Compute** in the left navigation pane and select *Open in new tab*.
-- MAGIC
-- MAGIC 1. Find the triangle icon to the right of your compute cluster name and click it.
-- MAGIC
-- MAGIC 1. Wait a few minutes for the cluster to start.
-- MAGIC
-- MAGIC 1. Once the cluster is running, complete the steps above to select your cluster.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## A. Classroom Setup
-- MAGIC
-- MAGIC Run the following cell to configure your working environment for this notebook.
-- MAGIC
-- MAGIC **NOTE:** The `DA` object is only used in Databricks Academy courses and is not available outside of these courses. It will dynamically reference the information needed to run the course in the lab environment.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-Auto-Loader

-- COMMAND ----------

-- MAGIC %md
-- MAGIC View the available file(s) in your `/Volumes/dbacademy/labuser/csv_files_autoloader_source` volume. Notice only one file exists.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f'LIST "/Volumes/dbacademy/{DA.schema_name}/csv_files_autoloader_source"').display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC [What is Auto Loader?](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/)
-- MAGIC
-- MAGIC [Using Auto Loader with Unity Catalog](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/unity-catalog)
-- MAGIC
-- MAGIC [Auto Loader options](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/options#csv-options)
-- MAGIC
-- MAGIC Below is an example of Auto Loader.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC ## Create a volume to store the Auto Loader checkpoint files
-- MAGIC spark.sql(f'CREATE VOLUME IF NOT EXISTS dbacademy.{DA.schema_name}.auto_loader_files')
-- MAGIC
-- MAGIC ## Set checkpoint location to the volume from above
-- MAGIC checkpoint_file_location = f'/Volumes/dbacademy/{DA.schema_name}/auto_loader_files'
-- MAGIC
-- MAGIC ## Incrementally (or stream) data using Auto Loader
-- MAGIC (spark
-- MAGIC  .readStream
-- MAGIC    .format("cloudFiles")
-- MAGIC    .option("cloudFiles.format", "csv")
-- MAGIC    .option("header", "true")
-- MAGIC    .option("sep", "|")
-- MAGIC    .option("inferSchema", "true")
-- MAGIC    .option("cloudFiles.schemaLocation", f"{checkpoint_file_location}")
-- MAGIC    .load(f"/Volumes/dbacademy/{DA.schema_name}/csv_files_autoloader_source/")
-- MAGIC  .writeStream
-- MAGIC    .option("checkpointLocation", f"{checkpoint_file_location}")
-- MAGIC    .trigger(once=True)
-- MAGIC    .toTable(f"dbacademy.{DA.schema_name}.python_csv_autoloader")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC View the new table **python_csv_autoloader**. Notice the data was ingested into a table and contains 3,149 rows.

-- COMMAND ----------

SELECT *
FROM python_csv_autoloader;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Add a CSV file to your `/Volumes/dbacademy/labuser/csv_files_autoloader_source/` volume.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC copy_files(copy_from = '/Volumes/dbacademy_ecommerce/v01/raw/sales-csv/', copy_to = f"/Volumes/dbacademy/{DA.schema_name}/csv_files_autoloader_source/", n=2)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Confirm your volume contains 2 CSV files.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f'LIST "/Volumes/dbacademy/{DA.schema_name}/csv_files_autoloader_source"').display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Rerun your Auto Loader ingestion from above (pasted for you below) to incrementally ingest only the new file.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC checkpoint_file_location = f'/Volumes/dbacademy/{DA.schema_name}/auto_loader_files'
-- MAGIC
-- MAGIC (spark
-- MAGIC  .readStream
-- MAGIC    .format("cloudFiles")
-- MAGIC    .option("cloudFiles.format", "csv")
-- MAGIC    .option("header", "true")
-- MAGIC    .option("sep", "|")
-- MAGIC    .option("inferSchema", "true")
-- MAGIC    .option("cloudFiles.schemaLocation", f"{checkpoint_file_location}")
-- MAGIC    .load(f"/Volumes/dbacademy/{DA.schema_name}/csv_files_autoloader_source/")
-- MAGIC  .writeStream
-- MAGIC    .option("checkpointLocation", f"{checkpoint_file_location}")
-- MAGIC    .trigger(once=True)
-- MAGIC    .toTable(f"dbacademy.{DA.schema_name}.python_csv_autoloader")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC View the **python_csv_autoloader** table. Notice that it now contains 6,081 rows.

-- COMMAND ----------

SELECT * 
FROM python_csv_autoloader;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC View the history of the **python_csv_autoloader** table. Notice the two **STREAMING UPDATES**. In the **operationMetrics** column you can see how many rows were ingestion in each streaming update. Notice that it only is ingestion new files.

-- COMMAND ----------

DESCRIBE HISTORY python_csv_autoloader;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Drop the **python_csv_autoloader** table.

-- COMMAND ----------

DROP TABLE IF EXISTS python_csv_autoloader;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>