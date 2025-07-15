-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Appendix - Ingesting XML Files with Databricks
-- MAGIC ### Extra material, not part of a live teach.
-- MAGIC In this demonstration we will go over how to ingest XML files and store them as Bronze Delta tables.
-- MAGIC
-- MAGIC ### Learning Objectives
-- MAGIC
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC
-- MAGIC - Use the `CREATE TABLE AS SELECT` (CTAS) statement with the `read_files()` function to ingest XML files into a Delta table, including any rescued data.
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

-- MAGIC %run ../Includes/Classroom-Setup-XML

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Run the cell below to view your default catalog and schema. Notice that your default catalog is **dbacademy** and your default schema is your unique **labuser** schema.
-- MAGIC
-- MAGIC **NOTE:** The default catalog and schema are pre-configured for you to avoid the need to specify the three-level name for when writing your tables (i.e., catalog.schema.table).

-- COMMAND ----------

SELECT current_catalog(), current_schema()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. CTAS with `read_files()` for Ingesting XML Files
-- MAGIC
-- MAGIC In this section, we'll explore how to ingest raw XML (Extensible Markup Language) files from cloud storage into a Delta table. XML files are structured text files that use custom tags to organize data.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC In the code below, we dynamically pass variables to `read_files()` since each user has a unique username within the Vocareum environment. This is done using the `DA` object created during classroom setup. 
-- MAGIC
-- MAGIC For example, the expression:
-- MAGIC
-- MAGIC `DA.paths_working_dir || '/xml_demo_files/example_1_data.xml'`
-- MAGIC
-- MAGIC evaluates to the string:
-- MAGIC
-- MAGIC `/Volumes/dbacademy/ops/<username>/xml_demo_files/example_1_data.xml`
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B1. View the XML File
-- MAGIC
-- MAGIC 1. Follow the steps below to view your XML file in your course volume: **dbacademy.ops.labuser**
-- MAGIC
-- MAGIC    a. In the left navigation bar, select the catalog icon:  ![Catalog Icon](../Includes/images/catalog_icon.png)
-- MAGIC
-- MAGIC    b. Expand the **dbacademy** catalog.
-- MAGIC
-- MAGIC    c. Expand the **ops** schema.
-- MAGIC
-- MAGIC    d. Expand **Volumes**. You should see a volume with your **labuser** name, which contains the source data to ingest.
-- MAGIC
-- MAGIC    e. Expand your **labuser** volume. This volume contains several subdirectories. We will use the **xml_demo_files** directory.
-- MAGIC
-- MAGIC    f. Expand the **xml_demo_files** subdirectory. It should contain the file: **example_1_data.xml**.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Since XML files can be difficult to work with, let's start this demonstration by looking at the **example_1_data.xml** XML file:
-- MAGIC
-- MAGIC ```
-- MAGIC   <books>
-- MAGIC     <book id="222">
-- MAGIC       <author>Corets, Eva</author>
-- MAGIC       <title>Maeve Ascendant</title>
-- MAGIC     </book>
-- MAGIC     <book id="333">
-- MAGIC       <author>Corets, Eva</author>
-- MAGIC       <title>Oberon's Legacy</title>
-- MAGIC     </book>
-- MAGIC   </books>
-- MAGIC ```
-- MAGIC
-- MAGIC This XML contains:
-- MAGIC - The Top level element `<books>`. This is the root element. It acts as a container for all the `<book>` elements.
-- MAGIC - Each `<book>` element represents a single book and includes:
-- MAGIC   - The `id` attribute which uniquely identifies the book.
-- MAGIC   - The `<author>` child element containing the name of the author. 
-- MAGIC   - The `<title>` child element containing the title of the book. 
-- MAGIC
-- MAGIC
-- MAGIC Our goal in ingesting this XML file is to flatten it into a tabular form so we can store it as a Delta table. We'll define the following columns:
-- MAGIC
-- MAGIC - **book_id** (extracted from the `id` attribute)
-- MAGIC - **author** (extracted from the `<author>` element text)
-- MAGIC - **title** (extracted from the `<title>` element text)
-- MAGIC
-- MAGIC Each `<book>` element will be treated as a row. To achieve this, we set `rowTag => 'book'` when using `read_files()`.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B2. Using the `read_files()` Function to Ingest XML
-- MAGIC
-- MAGIC The code in the next cell creates a structured table using a `CREATE TABLE AS SELECT` (CTAS) statement along with the `read_files()` function.
-- MAGIC
-- MAGIC We are using the following options with the `read_files()` function:
-- MAGIC
-- MAGIC 1. `format => "xml"` – Specifies that the input data is in XML format.  
-- MAGIC 2. `rowTag => "book"` – Identifies the repeating XML element (`<book>`) that defines individual rows.  
-- MAGIC 3. `schema => '_id INT, author STRING, title STRING'` – Enforces a schema for known fields in the XML.  
-- MAGIC 4. `rescuedDataColumn => '_rescued_data'` – Captures any malformed or unexpected fields that do not match the schema into a separate column for later inspection.
-- MAGIC
-- MAGIC This example demonstrates how to parse structured XML using schema enforcement while preserving problematic or unknown data for troubleshooting. For brevity, we skip the actual troubleshooting process.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Run the following cell to read in this XML file. The following query also brings in a `_rescued_data` column. Note that this column will return `NULL` because this XML file is made up of clean data.

-- COMMAND ----------

SELECT *
FROM read_files(
       DA.paths_working_dir || '/xml_demo_files/example_1_data.xml',
       format => "xml",
       rowTag => 'book',
       schema => '''
            _id INT, 
            author STRING, 
            title STRING
          ''',
       rescueddatacolumn => '_rescued_data'
     );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. We can also change the `rowTag` parameter to `"books"` to produce a different flattening of the XML file. 
-- MAGIC
-- MAGIC     In this case, we omit the explicit schema definition and allow schema inference. The resulting output is a single row containing a nested array.
-- MAGIC

-- COMMAND ----------

SELECT *
FROM read_files(
       DA.paths_working_dir || '/xml_demo_files/example_1_data.xml',
       format => "xml",
       rowTag => 'books',
       rescueddatacolumn => '_rescued_data'
     );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Let's finish by writing to Delta table based on the first schema presented above.

-- COMMAND ----------

-- Drop the table if it exists for demonstration purposes
DROP TABLE IF EXISTS books_bronze_xml;


-- Create the Delta table
CREATE TABLE books_bronze_xml 
AS
SELECT
  _id AS book_id,
  * EXCEPT (_id),
  current_timestamp AS ingestion_timestamp,
  _metadata.file_name AS source_file
FROM read_files(
       DA.paths_working_dir || '/xml_demo_files/example_1_data.xml',
       format => "xml",
       rowTag => 'book',
       schema => '''
            _id INT, 
            author STRING, 
            title STRING
          ''',
       rescuedDataColumn => '_rescued_data'
     );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Inspect the newly created **bronze** table.

-- COMMAND ----------

SELECT *
FROM books_bronze_xml

-- COMMAND ----------

-- View the datatypes of the columns 
DESCRIBE books_bronze_xml

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>