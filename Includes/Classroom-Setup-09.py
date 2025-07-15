# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG dbacademy;
# MAGIC USE SCHEMA IDENTIFIER(DA.schema_name);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample data for MERGE INTO
# MAGIC DROP TABLE IF EXISTS main_users_target;
# MAGIC -- Create the target table
# MAGIC CREATE OR REPLACE TABLE main_users_target (
# MAGIC     id INT,
# MAGIC     first_name STRING,
# MAGIC     email STRING,
# MAGIC     sign_up_date DATE,
# MAGIC     status STRING
# MAGIC );
# MAGIC -- Insert sample data into the target table
# MAGIC -- INSERT INTO main_users_target VALUES
# MAGIC -- (1, 'Panagiotis', 'panagiotis@example.com', '2024-01-01', 'current'),
# MAGIC -- (2, 'Samarth', 'samarth@example.com', '2024-01-05', 'current'),
# MAGIC -- (3, 'Zebi', 'zebi@example.com', '2024-01-10', 'current'),
# MAGIC -- (4, 'Mark', 'mark@leadinst.com', '2024-02-10', 'current');

# COMMAND ----------

spark.sql('''
INSERT INTO main_users_target VALUES
(1, 'Panagiotis', 'panagiotis@example.com', '2024-01-01', 'current'),
(2, 'Samarth', 'samarth@example.com', '2024-01-05', 'current'),
(3, 'Zebi', 'zebi@example.com', '2024-01-10', 'current'),
(4, 'Mark', 'mark@leadinst.com', '2024-02-10', 'current')
''')
print('Created the main_users_target table.')

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS update_users_source;
# MAGIC -- Create the source table
# MAGIC CREATE OR REPLACE TABLE update_users_source (
# MAGIC     id INT,
# MAGIC     first_name STRING,
# MAGIC     email STRING,
# MAGIC     sign_up_date DATE,
# MAGIC     status STRING
# MAGIC );
# MAGIC -- Insert sample data into the source table
# MAGIC
# MAGIC

# COMMAND ----------


spark.sql('''
INSERT INTO update_users_source VALUES
(1, 'Panagiotis', 'panagiotis@example.com', '2024-01-01', 'delete'),  -- User to delete
(2, 'Samarth', 'samarth123@newemail.com', '2024-01-05', 'update'),    -- Update Samarths's email
(5, 'Owen', 'owent@theemail.com', '2023-01-15', 'new'),               -- New user to insert
(6, 'Eva', 'ej@princessemail.com', '2023-01-15', 'new')               -- New user to insert
''')
print('Created the update_users_source table.')

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS new_users_source;
# MAGIC -- Create the source table
# MAGIC CREATE OR REPLACE TABLE new_users_source (
# MAGIC     id INT,
# MAGIC     first_name STRING,
# MAGIC     email STRING,
# MAGIC     sign_up_date DATE,
# MAGIC     status STRING,
# MAGIC     country STRING
# MAGIC );
# MAGIC -- Insert sample data into the source table

# COMMAND ----------

spark.sql('''
INSERT INTO new_users_source VALUES
(7, 'Kristi', 'kristi@theemail.com', '2023-01-15', 'new', 'USA'),               -- New user to insert
(8, 'Mohammad', 'mohammad@princessemail.com', '2023-01-15', 'new','Pakistan'),  -- New user to insert
(9, 'Christos', 'christos@example.com', '2024-01-01', 'new','Greece');          -- New user to insert
''')
print('Created the new_users_source table.')