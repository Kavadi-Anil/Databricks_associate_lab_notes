# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Data Ingestion with Lakeflow Connect
# MAGIC
# MAGIC This course provides a comprehensive introduction to Lakeflow Connect as a scalable and simplified solution for ingesting data into Databricks from a variety of data sources. You will begin by exploring the different types of connectors within Lakeflow Connect (Standard and Managed), learn about various ingestion techniques, including batch, incremental batch, and streaming, and then review the key benefits of Delta tables and the Medallion architecture.
# MAGIC
# MAGIC From there, you will gain practical skills to efficiently ingest data from cloud object storage using Lakeflow Connect Standard Connectors with methods such as CREATE TABLE AS (CTAS), COPY INTO, and Auto Loader, along with the benefits and considerations of each approach. You will then learn how to append metadata columns to your bronze level tables during ingestion into the Databricks data intelligence platform. This is followed by working with the rescued data column, which handles records that don’t match the schema of your bronze table, including strategies for managing this rescued data.
# MAGIC
# MAGIC The course also introduces techniques for ingesting and flattening semi-structured JSON data, as well as enterprise-grade data ingestion using Lakeflow Connect Managed Connectors.
# MAGIC
# MAGIC Finally, learners will explore alternative ingestion strategies, including MERGE INTO operations and leveraging the Databricks Marketplace, equipping you with foundational knowledge to support modern data engineering ingestion.
# MAGIC
# MAGIC ------
# MAGIC
# MAGIC ### Prerequisites
# MAGIC You should meet the following prerequisites before starting this course:
# MAGIC
# MAGIC - Basic understanding of the Databricks Data Intelligence platform, including Databricks Workspaces, Apache Spark, Delta Lake, the Medallion Architecture and Unity Catalog.
# MAGIC - Experience working with various file formats (e.g., Parquet, CSV, JSON, TXT).
# MAGIC - Proficiency in SQL and Python.
# MAGIC - Familiarity with running code in Databricks notebooks.
# MAGIC
# MAGIC ---
# MAGIC ### Course Agenda
# MAGIC The following modules are part of the **Data Engineer Learning** Path by Databricks Academy.
# MAGIC | # | Notebook Name |
# MAGIC | --- | --- |
# MAGIC | 00 | [Appendix]($./Appendix) |
# MAGIC | 01 | [Exploring the Lab Environment]($./01 - Exploring the Lab Environment) |
# MAGIC | 02A | [Data Ingestion with CREATE TABLE AS and COPY INTO]($./02A - Data Ingestion with CREATE TABLE AS and COPY INTO) |
# MAGIC | 02B | [SQL and Auto Loader - CREATE STREAMING TABLES]($./02B -  Create Streaming Tables with SQL using Auto Loader) |
# MAGIC | 03 | [Adding Metadata Columns During Ingestion]($./03 - Adding Metadata Columns During Ingestion) |
# MAGIC | 04 | [Handling CSV Ingestion with the Rescued Data Column]($./04 - Handling CSV Ingestion with the Rescued Data Column) |
# MAGIC | 05L | [Creating Bronze Tables from CSV Files]($./05L - Creating Bronze Tables from CSV Files) |
# MAGIC | 06 | [Ingesting JSON files with Databricks]($./06 - Ingesting JSON files with Databricks) |
# MAGIC | 07L | [Creating Bronze Tables from JSON Files]($./07L - Creating Bronze Tables from JSON Files) |
# MAGIC | 08 | [Enterprise Data Ingestion with Lakeflow Connect]($./08 - Enterprise Data Ingestion with Lakeflow Connect) |
# MAGIC | 09 | [BONUS - Data Ingestion with MERGE INTO]($./09 - BONUS - Data Ingestion with MERGE INTO) |
# MAGIC
# MAGIC --- 
# MAGIC
# MAGIC ### Requirements
# MAGIC
# MAGIC Please review the following requirements before starting the lesson:
# MAGIC
# MAGIC * To run demo and lab notebooks, you need to use the following Databricks runtime: **`16.4.x-scala2.12`**
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>