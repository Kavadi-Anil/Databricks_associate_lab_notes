-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 8 - Enterprise Data Ingestion with LakeFlow Connect
-- MAGIC
-- MAGIC In this demonstration, you will be introduced to **LakeFlow Connect** managed connectors for external enterprise sources. An easy-to-use tool for bringing data from various systems such as databases (e.g., SQL Server), applications (e.g., Salesforce, Workday), and file storage services (e.g., SharePoint) into the lakehouse. 
-- MAGIC
-- MAGIC It is designed to handle data efficiently and automatically improve performance.
-- MAGIC
-- MAGIC ### Learning Objectives
-- MAGIC
-- MAGIC By the end of the demonstration, you should be able to:
-- MAGIC
-- MAGIC - Explore the available Databricks managed connectors to ingest data through LakeFlow Connect.
-- MAGIC - View how to ingest data using Partner connect.
-- MAGIC - Add a Database connector and configure an ingestion pipeline demonstration:
-- MAGIC   - Choose which data you want to ingest and synchronize to Databricks
-- MAGIC   - Synchronize the data pipeline to Unity Catalog (catalog and schema)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Complete the following to view the Lakeflow Connect documentation:
-- MAGIC
-- MAGIC    a. Go to the [Databricks documentation](https://docs.databricks.com/aws/en/) page.
-- MAGIC
-- MAGIC    b. In the left navigation bar, expand **Data Engineering**.
-- MAGIC
-- MAGIC    c. Expand **Lakeflow Connect** to view the available documentation for data ingestion with Lakeflow Connect.
-- MAGIC
-- MAGIC    d. Expand **Managed Connectors** to access information about Databricks-managed connectors.
-- MAGIC
-- MAGIC **NOTE:** Please note that the documentation directions may change during an update.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Complete the following to explore the available **Data Ingestion** capabilities in Databricks:
-- MAGIC
-- MAGIC    a. On the left main navigation bar, right-click on **Data Ingestion** and select **Open in a New Tab**.
-- MAGIC
-- MAGIC    b. Under **Databricks Connectors**, you'll see various Databricks-managed connectors provided by LakeFlow Connect.
-- MAGIC
-- MAGIC    c. Under the **Files** section, you can:
-- MAGIC    
-- MAGIC    - **Create or modify tables**
-- MAGIC
-- MAGIC    - **Upload files to a volume**
-- MAGIC
-- MAGIC    - **Create a table from Amazon S3**
-- MAGIC    
-- MAGIC    - Drag and drop files when using the options to create/modify tables or upload files to a volume.
-- MAGIC
-- MAGIC    d. Under **Fivetran Connectors**, you can search for specific data source connections using a partner.
-- MAGIC
-- MAGIC    e. Click on any connector to view details.
-- MAGIC
-- MAGIC    f. You can also upload files directly to **DBFS** for backward compatibility, though Databricks recommends uploading to Unity Catalog moving forward.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Complete the following point-and-click demonstration to learn how to use a Databricks managed connector to an external database or SaaS application using **LakeFlow Connect**.
-- MAGIC
-- MAGIC       **NOTE:** This course does not have an active database or SaaS application to use. The demos below are a simple walk through. During a live teach, pick one of the tours below.
-- MAGIC
-- MAGIC    - [LakeFlow Connect Managed Connector Demonstration](https://app.getreprise.com/launch/BXZY58n/) - Simple demonstration
-- MAGIC
-- MAGIC    - [Databricks Lakeflow Connect for Workday Reports: Connect, Ingest, and Analyze Workday Data Without Complexity](https://www.databricks.com/resources/demos/tours/appdev/lakeflow-workday-connect?itm_data=demo_center)
-- MAGIC
-- MAGIC    - [Databricks Lakeflow Connect for Salesforce: Powering Smarter Selling with AI and Analytics](https://www.databricks.com/resources/demos/tours/platform/discover-databricks-lakeflow-connect-demo?itm_data=demo_center)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>