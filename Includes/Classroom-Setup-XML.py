# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG dbacademy;
# MAGIC USE SCHEMA IDENTIFIER(DA.schema_name);

# COMMAND ----------

# Create a sample xml file in xml volume

xmlString = """
  <books>
    <book id="222">
      <author>Corets, Eva</author>
      <title>Maeve Ascendant</title>
    </book>
    <book id="333">
      <author>Corets, Eva</author>
      <title>Oberon's Legacy</title>
    </book>
  </books>"""


username_cleaned = DA.username.replace('.', '_')
xmlPath = f"/Volumes/dbacademy/ops/{username_cleaned}/xml_demo_files/example_1_data.xml"

r = dbutils.fs.put(xmlPath, xmlString, True)