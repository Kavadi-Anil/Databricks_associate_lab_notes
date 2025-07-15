-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 6 - Ingesting JSON Files with Databricks
-- MAGIC
-- MAGIC In this demonstration, we’ll explore how to ingest JSON files and perform foundational JSON-specific transformations during ingestion, including decoding encoded fields and flattening nested JSON strings. We’ll be working with simulated Kafka event data, sourced from Databricks Marketplace.
-- MAGIC
-- MAGIC ### Learning Objectives
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC - Ingest raw JSON data into Unity Catalog using CTAS and `read_files()`.
-- MAGIC - Apply multiple techniques to flatten JSON string columns with and without converting to a STRUCT type.
-- MAGIC - Understand the difference between `explode()` and `explode_outer()`.
-- MAGIC - Introduce the capabilities and use cases of the VARIANT data type (public preview as of Q2-2025)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## REQUIRED - SELECT CLASSIC COMPUTE
-- MAGIC
-- MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default and you have a Serverless SQL warehouse with a similar name.
-- MAGIC
-- MAGIC ![Select Cluster](./Includes/images/selecting_cluster_info.png)
-- MAGIC
-- MAGIC Follow these steps to select the classic compute cluster:
-- MAGIC
-- MAGIC
-- MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**.
-- MAGIC
-- MAGIC 2. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
-- MAGIC
-- MAGIC    - Click **More** in the drop-down.
-- MAGIC
-- MAGIC    - In the **Attach to an existing compute resource** window, use the first drop-down to select your unique cluster.
-- MAGIC
-- MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
-- MAGIC
-- MAGIC 1. Right-click on **Compute** in the left navigation pane and select *Open in new tab*.
-- MAGIC
-- MAGIC 2. Find the triangle icon to the right of your compute cluster name and click it.
-- MAGIC
-- MAGIC 3. Wait a few minutes for the cluster to start.
-- MAGIC
-- MAGIC 4. Once the cluster is running, complete the steps above to select your cluster.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## A. Classroom Setup
-- MAGIC
-- MAGIC Run the following cell to configure your working environment for this notebook.
-- MAGIC
-- MAGIC **NOTE:** The `DA` object is only used in Databricks Academy courses and is not available outside of these courses. It will dynamically reference the information needed to run the course in the lab environment.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-06

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Run the cell below to view your default catalog and schema. Notice that your default catalog is **dbacademy** and your default schema is your unique **labuser** schema.
-- MAGIC
-- MAGIC **NOTE:** The default catalog and schema are pre-configured for you to avoid the need to specify the three-level name for when writing your tables (i.e., catalog.schema.table).

-- COMMAND ----------

SELECT current_catalog(), current_schema()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Overview of CTAS with `read_files()` for Ingestion of JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B1. Inspect JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Run the next cell to verify that there are 11 JSON files located at `/Volumes/dbacademy_ecommerce/v01/raw/events-kafka`.

-- COMMAND ----------

-- DBTITLE 1,List files in volume
LIST '/Volumes/dbacademy_ecommerce/v01/raw/events-kafka'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Run the cell below to view the raw JSON data in the output. Note the following:
-- MAGIC
-- MAGIC    - Each row contains JSON with 6 key/value pairs.
-- MAGIC
-- MAGIC    - The **key** and **value** fields are encoded in base64. Base64 is an encoding scheme that converts binary data into a readable ASCII string.
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC <br></br>
-- MAGIC **Example Output Formatted**
-- MAGIC ```
-- MAGIC {
-- MAGIC     "key": "VUEwMDAwMDAxMDczOTgwNTQ=",
-- MAGIC     "offset": 219255030,
-- MAGIC     "partition": 0,
-- MAGIC     "timestamp": 1593880885085,
-- MAGIC     "topic": "clickstream",
-- MAGIC     "value": "eyJkZXZpY2UiOiJBbmRyb2lkIiwiZWNvbW1lcmNlIjp7fSwiZXZlbnRfbmFtZSI6Im1haW4iLCJldmVudF90aW1lc3R
-- MAGIC     hbXAiOjE1OTM4ODA4ODUwMzYxMjksImdlbyI6eyJjaXR5IjoiTmV3IFlvcmsiLCJzdGF0ZSI6Ik5ZIn0sIml0ZW1zIjp
-- MAGIC     bXSwidHJhZmZpY19zb3VyY2UiOiJnb29nbGUiLCJ1c2VyX2ZpcnN0X3RvdWNoX3RpbWVzdGFtcCI6MTU5Mzg4MDg4NTA
-- MAGIC     zNjEyOSwidXNlcl9pZCI6IlVBMDAwMDAwMTA3Mzk4MDU0In0=",
-- MAGIC }
-- MAGIC ```

-- COMMAND ----------

-- DBTITLE 1,View JSON files as text
SELECT * 
FROM text.`/Volumes/dbacademy_ecommerce/v01/raw/events-kafka`
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Run the cell below to see how to use `read_files()` to read the JSON data. Notice the following:
-- MAGIC
-- MAGIC    - The JSON file is cleanly read into a tabular format with 6 columns.
-- MAGIC
-- MAGIC    - The **key** and **value** columns are base64-encoded and returned as STRING data type.
-- MAGIC    
-- MAGIC    - There are no rows in the **_rescued_data** column.

-- COMMAND ----------

-- DBTITLE 1,View JSON file in tabular form
SELECT *
FROM read_files(
  "/Volumes/dbacademy_ecommerce/v01/raw/events-kafka",
  format => "json"
)
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B2. Using CTAS and `read_files()` with JSON
-- MAGIC
-- MAGIC Ingesting JSON files using `read_files()` is as straightforward as reading CSV files.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Run the cell below to store this raw data in the **kafka_events_bronze_raw** table and view the table. When inspecting the results, you'll notice that:
-- MAGIC
-- MAGIC    - The **key** and **value** columns are of type STRING and contain data that is **base64-encoded**.
-- MAGIC
-- MAGIC    - This means the actual content has been encoded into base64 format and stored as a string. 
-- MAGIC    
-- MAGIC    - They have not yet been transformed into a readable string in the first bronze table we create.
-- MAGIC
-- MAGIC **NOTE:** Base64 encoding is commonly used when ingesting data from sources like message queues or streaming platforms, where preserving formatting and avoiding data corruption is important.

-- COMMAND ----------

-- DBTITLE 1,Create the bronze raw from JSON
-- Drop the table if it exists for demonstration purposes
DROP TABLE IF EXISTS kafka_events_bronze_raw;


-- Create the Delta table
CREATE TABLE kafka_events_bronze_raw AS
SELECT *
FROM read_files(
  "/Volumes/dbacademy_ecommerce/v01/raw/events-kafka",
  format => "json"
);


-- Display the table
SELECT *
FROM kafka_events_bronze_raw
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B3. Decoding base64 Strings for the Bronze Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Let's take a look at decoding the **key** and **value** columns by inspecting their data types after applying the `unbase64()` function. The `unbase64` function returns a decoded base64 string as binary.
-- MAGIC
-- MAGIC     - **encoded_key**: The original encoded **key** column as a base64 string.
-- MAGIC
-- MAGIC     - **decoded_key**: A new column created by decoding **key** from a base64 string to BINARY.
-- MAGIC
-- MAGIC     - **encoded_value**: The original encoded **value** column as a base64 string.
-- MAGIC
-- MAGIC     - **decoded_value**: A new column created by decoding **value** from a base64 string to BINARY.
-- MAGIC
-- MAGIC     Run the cell and view the results. Notice that the **decoded_key** and **decoded_value** columns are now BINARY.

-- COMMAND ----------

-- DBTITLE 1,Decode the Base64 string as binary
SELECT
  key AS encoded_key,
  unbase64(key) AS decoded_key,
  value AS encoded_value,
  unbase64(value) AS decoded_value
FROM kafka_events_bronze_raw
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Run the next cell to convert the BINARY columns to STRING columns using the `CAST` function. Notice the following in the results:
-- MAGIC
-- MAGIC     - The **decoded_key** and **decoded_value** columns are now of type STRING and readable.
-- MAGIC
-- MAGIC     - The **decoded_value** column is a JSON-formatted string.
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Cast the BINARY as a STRING with CAST
SELECT
  key AS encoded_key,
  cast(unbase64(key) AS STRING) AS decoded_key,
  value AS encoded_value,
  cast(unbase64(value) AS STRING) AS decoded_value
FROM kafka_events_bronze_raw
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Now, let's put it all together to create another bronze-level table named **kafka_events_bronze_decoded**. This table will store the STRING values for the **key** and **value** columns from the original **kafka_events_bronze_raw** table.

-- COMMAND ----------

-- DBTITLE 1,Create the bronze_decoded table
CREATE OR REPLACE TABLE kafka_events_bronze_decoded AS
SELECT
  cast(unbase64(key) AS STRING) AS decoded_key,
  offset,
  partition,
  timestamp,
  topic,
  cast(unbase64(value) AS STRING) AS decoded_value
FROM kafka_events_bronze_raw;


-- View the new table
SELECT *
FROM kafka_events_bronze_decoded
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C. Working with JSON Formatted Strings in a Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C1. Flattening JSON String Columns
-- MAGIC
-- MAGIC Next, we will explore how extract a column from a column containing a JSON formatted string. 
-- MAGIC
-- MAGIC
-- MAGIC **BENEFITS**
-- MAGIC - **Simple** - Easy to implement and store JSON as plain text.
-- MAGIC - **Flexible** - Can hold any JSON structure without schema constraints.
-- MAGIC
-- MAGIC **CONSIDERATIONS**
-- MAGIC - **Performance** - STRING columns are slower when querying and processing complex data.
-- MAGIC - **No Schema** - The lack of a defined schema for STRING columns can lead to data integrity issues.
-- MAGIC - **Complex to Query** - Requires additional code to parse and retrieve data, which can be complex.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### C1.1 Query JSON strings
-- MAGIC
-- MAGIC You can extract a column from fields containing JSON strings using the syntax: `<column-name>:<extraction-path>`, where `<column-name>` is the string column name and `<extraction-path>` is the path to the field to extract. The returned results are strings. You can also do this with nested fields by using either `.` or `[]`.
-- MAGIC
-- MAGIC This utilizes Spark SQL's built-in functionality to interact directly with nested data stored as JSON strings.
-- MAGIC
-- MAGIC [Query JSON strings](https://docs.databricks.com/aws/en/semi-structured/json)
-- MAGIC
-- MAGIC
-- MAGIC Example JSON string pulled from a row in the column **decoded_value**:
-- MAGIC
-- MAGIC
-- MAGIC ```
-- MAGIC {
-- MAGIC     "device": "iOS",
-- MAGIC     "ecommerce": {},
-- MAGIC     "event_name": "add_item",
-- MAGIC     "event_previous_timestamp": 1593880300696751,
-- MAGIC     "event_timestamp": 1593880892251310,
-- MAGIC     "geo": {
-- MAGIC       "city": "Westbrook", 
-- MAGIC       "state": "ME"
-- MAGIC       },
-- MAGIC     "items": [
-- MAGIC         {
-- MAGIC             "item_id": "M_STAN_T",
-- MAGIC             "item_name": "Standard Twin Mattress",
-- MAGIC             "item_revenue_in_usd": 595.0,
-- MAGIC             "price_in_usd": 595.0,
-- MAGIC             "quantity": 1,
-- MAGIC         }
-- MAGIC     ],
-- MAGIC     "traffic_source": "google",
-- MAGIC     "user_first_touch_timestamp": 1593880300696751,
-- MAGIC     "user_id": "UA000000107392458",
-- MAGIC }
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. For example, let's extract the following values from the JSON-formatted string:
-- MAGIC     - `decoded_value:device`
-- MAGIC     - `decoded_value:traffic_source`
-- MAGIC     - `decoded_value:geo`
-- MAGIC     - `decoded_value:items`
-- MAGIC
-- MAGIC     Run the cell and view the results. Notice that we have successfully extracted the values from the JSON formatted string.
-- MAGIC
-- MAGIC     - **device** is a STRING
-- MAGIC
-- MAGIC     - **traffic_source** is a STRING
-- MAGIC
-- MAGIC     - **geo** is a STRING containing another JSON formatted string
-- MAGIC     
-- MAGIC     - **item** is a STRING contain an array of JSON formatted strings
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Query a JSON string and extract values
SELECT 
  decoded_value,
  decoded_value:device,
  decoded_value:traffic_source,
  decoded_value:geo,       ----- Contains another JSON formatted string
  decoded_value:items      ----- Contains a nested-array of JSON formatted strings
FROM kafka_events_bronze_decoded
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. We can then begin to parse out the necessary JSON formatted string values to create another bronze table to flatten the JSON formatted string column for downstream processing.

-- COMMAND ----------

-- DBTITLE 1,Flatten the JSON formatted string
CREATE OR REPLACE TABLE kafka_events_bronze_string_flattened AS
SELECT
  decoded_key,
  offset,
  partition,
  timestamp,
  topic,
  decoded_value:device,
  decoded_value:traffic_source,
  decoded_value:geo,       ----- Contains another JSON formatted string
  decoded_value:items      ----- Contains a nested-array of JSON formatted strings
FROM kafka_events_bronze_decoded;


-- Display the table
SELECT *
FROM kafka_events_bronze_string_flattened;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C2. Flattening JSON Formatting Strings via STRUCT Conversion
-- MAGIC
-- MAGIC Similar to the previous section, we will discuss how to flatten our JSON STRING column **decoded_value** using a STRUCT column.
-- MAGIC
-- MAGIC #### Benefits and Considerations of STRUCT Columns
-- MAGIC
-- MAGIC **Benefits**
-- MAGIC - **Schema Enforcement** – STRUCT columns define and enforce a schema, helping maintain data integrity.
-- MAGIC - **Improved Performance** – STRUCTs are generally more efficient for querying and processing than plain strings.
-- MAGIC
-- MAGIC **Considerations**
-- MAGIC - **Schema Enforcement** – Because the schema is enforced, issues can arise if the JSON structure changes over time.
-- MAGIC - **Reduced Flexibility** – The data must consistently match the defined schema, leaving less room for structural variation.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### C2.1 Converting a JSON STRING to a STRUCT Column
-- MAGIC To convert a JSON-formatted STRING column to a STRUCT column, you will need to derive the schema of the JSON-formatted string and then parse each row into a STRUCT type.
-- MAGIC
-- MAGIC We can this in two steps.
-- MAGIC   1. Get the STRUCT type of the JSON formatted string.
-- MAGIC   2. Apply the STRUCT to the JSON formatted string column.
-- MAGIC
-- MAGIC **NOTE:** We have already copied and pasted the correct values for you as a part of this demonstration. The subsequent cell below is a copy and paste of the output of the single row that appears when running the next cell. 
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Determine the derived schema using the [**`schema_of_json()`**](https://docs.databricks.com/en/sql/language-manual/functions/schema_of_json.html) function, which returns the schema inferred from a JSON-formatted string.
-- MAGIC
-- MAGIC     Run the cell and view the results. Notice that the output displays the structure of the JSON string.

-- COMMAND ----------

-- DBTITLE 1,Determine the schema of the JSON formatted string
SELECT schema_of_json('{"device":"Linux","ecommerce":{"purchase_revenue_in_usd":1075.5,"total_item_quantity":1,"unique_items":1},"event_name":"finalize","event_previous_timestamp":1593879231210816,"event_timestamp":1593879335779563,"geo":{"city":"Houston","state":"TX"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_K","item_name":"Standard King Mattress","item_revenue_in_usd":1075.5,"price_in_usd":1195.0,"quantity":1}],"traffic_source":"email","user_first_touch_timestamp":1593454417513109,"user_id":"UA000000106116176"}')
AS schema

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Copy and paste the output from `schema_of_json` into the [**`from_json()`**](https://docs.databricks.com/en/sql/language-manual/functions/from_json.html) function. This function parses a column containing a JSON-formatted string into a STRUCT type using the specified schema, and creates a new table named **kafka_events_bronze_struct**.
-- MAGIC
-- MAGIC     Run the cell and view the results. Notice that the **value** column has been transformed into a nested STRUCT that includes scalar fields, nested structs, and an array of structs.

-- COMMAND ----------

CREATE OR REPLACE TABLE kafka_events_bronze_struct AS
SELECT 
  * EXCEPT (decoded_value),
  from_json(
      decoded_value,    -- JSON formatted string column
      'STRUCT<device: STRING, ecommerce: STRUCT<purchase_revenue_in_usd: DOUBLE, total_item_quantity: BIGINT, unique_items: BIGINT>, event_name: STRING, event_previous_timestamp: BIGINT, event_timestamp: BIGINT, geo: STRUCT<city: STRING, state: STRING>, items: ARRAY<STRUCT<coupon: STRING, item_id: STRING, item_name: STRING, item_revenue_in_usd: DOUBLE, price_in_usd: DOUBLE, quantity: BIGINT>>, traffic_source: STRING, user_first_touch_timestamp: BIGINT, user_id: STRING>') AS value
FROM kafka_events_bronze_decoded;


-- View the new table.
SELECT *
FROM kafka_events_bronze_struct
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### C2.2 Extract fields, nested fields, and nested arrays from STRUCT columns
-- MAGIC
-- MAGIC We can query the STRUCT column using `value.device` or `value.ecommerce` in our SELECT statement.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Using this syntax, we can obtain values from the **value** struct column. Run the cell and view the results. Notice the following:
-- MAGIC
-- MAGIC     - We obtained values from the STRUCT column for **device** and **city**
-- MAGIC     
-- MAGIC     - The **items** column contains an ARRAY of STRUCTS. The number of elements in the array varies.

-- COMMAND ----------

-- DBTITLE 1,Obtain values from a STRUCT column
SELECT 
  decoded_key,
  value.device as device,  -- <----- Field
  value.geo.city as city,  -- <----- Nested-field from geo field
  value.items as items,
  array_size(items) AS number_elements_in_array -- <----- Count the number of elements in the array column items
FROM kafka_events_bronze_struct
ORDER BY number_elements_in_array DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### C2.3 Explode Arrays
-- MAGIC
-- MAGIC Exploding an array transforms each element of an array column into a separate row, effectively flattening the array. There are a few things to keep in mind when using this function. 
-- MAGIC
-- MAGIC 1. It returns a set of rows composed of the elements of the array or the keys and values of the map.
-- MAGIC
-- MAGIC 1. If the array is `NULL` no rows are produced. To return a single row with `NULL`s for the array or map values use the [`explode_outer()`](https://docs.databricks.com/gcp/en/sql/language-manual/functions/explode_outer) function.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Run the cell to see how the ARRAY of values in the `value.items` explodes the array into one row for each element in the array.

-- COMMAND ----------

-- DBTITLE 1,Explore the array to one row per element
CREATE OR REPLACE TABLE bronze_explode_array AS
SELECT
  decoded_key,
  array_size(value.items) AS number_elements_in_array,
  explode(value.items) AS item_in_array,
  value.items
FROM kafka_events_bronze_struct
ORDER BY number_elements_in_array DESC;


-- Display table
SELECT *
FROM bronze_explode_array;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## D. Working with a VARIANT Column (Public Preview)
-- MAGIC
-- MAGIC #### VARIANT Column Benefits and Considerations:
-- MAGIC
-- MAGIC **BENEFITS**
-- MAGIC - **Open** - Fully open-sourced, no proprietary data lock-in.
-- MAGIC - **Flexible** - No strict schema. You can put any type of semi-structured data into VARIANT.
-- MAGIC - **Performant** - Improved performance over existing methods.
-- MAGIC
-- MAGIC **CONSIDERATIONS**
-- MAGIC - Currently in public preview as of 2025 Q2.
-- MAGIC - [Variant support in Delta Lake](https://docs.databricks.com/aws/en/delta/variant)
-- MAGIC
-- MAGIC **RESOURCES**:
-- MAGIC - [Introducing the Open Variant Data Type in Delta Lake and Apache Spark](https://www.databricks.com/blog/introducing-open-variant-data-type-delta-lake-and-apache-spark)
-- MAGIC - [Say goodbye to messy JSON headaches with VARIANT](https://www.youtube.com/watch?v=fWdxF7nL3YI)
-- MAGIC - [Variant Data Type - Making Semi-Structured Data Fast and Simple](https://www.youtube.com/watch?v=jtjOfggD4YY)
-- MAGIC
-- MAGIC
-- MAGIC **NOTE:** Variant data type will not work on Serverless Version 1.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. View the **kafka_events_bronze_decoded** table. Confirm the **decoded_value** column contains a JSON formatted string.

-- COMMAND ----------

SELECT *
FROM kafka_events_bronze_decoded
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Use the [`parse_json`](https://docs.databricks.com/aws/en/sql/language-manual/functions/parse_json) function to returns a VARIANT value from the JSON formatted string.
-- MAGIC
-- MAGIC     Run the cell and view the results. Notice that the **json_variant_value** column is of type VARIANT.

-- COMMAND ----------

-- DBTITLE 1,Create a VARIANT column
CREATE OR REPLACE TABLE kafka_events_bronze_variant AS
SELECT
  decoded_key,
  offset,
  partition,
  timestamp,
  topic,
  parse_json(decoded_value) AS json_variant_value   -- Convert the decoded_value column to a variant data type
FROM kafka_events_bronze_decoded;

-- View the table
SELECT *
FROM kafka_events_bronze_variant
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. You can parse the VARIANT data type column using `:` to create your desired table.
-- MAGIC
-- MAGIC     [VARIANT type](https://docs.databricks.com/aws/en/sql/language-manual/data-types/variant-type)

-- COMMAND ----------

SELECT
  json_variant_value,
  json_variant_value:device :: STRING,  -- Obtain the value of device and cast to a string
  json_variant_value:items
FROM kafka_events_bronze_variant
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>