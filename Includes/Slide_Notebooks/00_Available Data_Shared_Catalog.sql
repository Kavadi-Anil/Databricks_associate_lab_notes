-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 1 - UC Exploration and Simple Data Ingestion
-- MAGIC
-- MAGIC After extracting data from external data sources, load data into the Lakehouse to ensure that all of the benefits of the Databricks platform can be fully leveraged.
-- MAGIC
-- MAGIC While different organizations may have varying policies for how data is initially loaded into Databricks, we typically recommend that early tables represent a mostly raw version of the data, and that validation and enrichment occur in later stages. This pattern ensures that even if data doesn't match expectations with regards to data types or column names, no data will be dropped, meaning that programmatic or manual intervention can still salvage data in a partially corrupted or invalid state.
-- MAGIC
-- MAGIC This lesson will focus primarily on the **`CREATE TABLE _ AS SELECT`** (CTAS) and **`COPY INTO`** statements.
-- MAGIC
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC - Explore available files in Databricks volumes
-- MAGIC - Use CTAS statements to create Delta Lake tables
-- MAGIC - Create new tables from existing views or tables
-- MAGIC - Enrich loaded data with additional metadata
-- MAGIC - Declare table schema with generated columns and descriptive comments
-- MAGIC - Set advanced options to control data location, quality enforcement, and partitioning
-- MAGIC - Create shallow and deep clones

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Run Setup
-- MAGIC
-- MAGIC The setup script will create the data and declare necessary values for the rest of this notebook to execute.
-- MAGIC

-- COMMAND ----------

-- Create the table
CREATE TABLE StudyScores (
    HoursStudied INT,
    TestScore INT
);

-- Insert data into the table
INSERT INTO StudyScores (HoursStudied, TestScore) VALUES
(1, 50),
(2, 55),
(3, 65),
(4, 70),
(5, 80);

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-1

-- COMMAND ----------

LIST '${DA.paths.datasets.ecommerce}/raw'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Events Data

-- COMMAND ----------

SELECT *
FROM json.`/Volumes/dbacademy_ecommerce/v01/raw/events-500k-json/`
LIMIT 10;

-- COMMAND ----------

SELECT *
FROM parquet.`/Volumes/dbacademy_ecommerce/v01/raw/events-historical/`
LIMIT 10;

-- COMMAND ----------

SELECT *
FROM json.`/Volumes/dbacademy_ecommerce/v01/raw/events-kafka/`
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Item  Data

-- COMMAND ----------

SELECT *
FROM parquet.`/Volumes/dbacademy_ecommerce/v01/raw/item-lookup/`
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Products Data

-- COMMAND ----------

SELECT *
FROM csv.`/Volumes/dbacademy_ecommerce/v01/raw/products-csv/`
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Sales Data

-- COMMAND ----------

SELECT *
FROM parquet.`/Volumes/dbacademy_ecommerce/v01/raw/sales-30m/`
LIMIT 10;

-- COMMAND ----------

SELECT *
FROM csv.`/Volumes/dbacademy_ecommerce/v01/raw/sales-csv/`
LIMIT 10;

-- COMMAND ----------

SELECT *
FROM parquet.`/Volumes/dbacademy_ecommerce/v01/raw/sales-historical/`
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Users Data

-- COMMAND ----------

SELECT *
FROM parquet.`/Volumes/dbacademy_ecommerce/v01/raw/users-30m/`
LIMIT 10;

-- COMMAND ----------

SELECT count(*)
FROM parquet.`/Volumes/dbacademy_ecommerce/v01/raw/users-30m/`;

-- COMMAND ----------

SELECT *
FROM csv.`/Volumes/dbacademy_ecommerce/v01/raw/users-500k-csv/`
LIMIT 10;

-- COMMAND ----------

SELECT count(*)
FROM csv.`/Volumes/dbacademy_ecommerce/v01/raw/users-500k-csv/`;

-- COMMAND ----------

SELECT *
FROM parquet.`/Volumes/dbacademy_ecommerce/v01/raw/users-historical/`
LIMIT 10;

-- COMMAND ----------

SELECT count(*)
FROM parquet.`/Volumes/dbacademy_ecommerce/v01/raw/users-historical/`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>