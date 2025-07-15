# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG dbacademy;
# MAGIC USE SCHEMA IDENTIFIER(DA.schema_name);

# COMMAND ----------

import json
import base64
import random
from datetime import datetime
from pathlib import Path


# Function to create one event with nested array
def create_event(i: int) -> dict:
    value_dict = {
        "user_id": f"user_{random.randint(1000, 9999)}",
        "event_type": random.choice(["click", "purchase", "view"]),
        "event_timestamp": datetime.now().isoformat(),
        "items": [
            {
                "item_id": f"item_{random.randint(100, 999)}",
                "quantity": random.randint(1, 5),
                "price_usd": round(random.uniform(10.0, 100.0), 2)
            }
            for _ in range(random.randint(1, 3))  # 1 to 3 items per event
        ]
    }

    # Encode value as JSON string, then base64
    encoded_value = base64.b64encode(json.dumps(value_dict).encode()).decode()

    return {
        "key": f"event_{i}",
        "timestamp": datetime.now().timestamp(),
        "value": encoded_value
    }

# Create 100 records
events = [create_event(i) for i in range(100)]


username_cleaned = DA.username.replace('.', '_')
# Output file path
output_path = Path(f'/Volumes/dbacademy/ops/{username_cleaned}/json_demo_files/lab_kafka_events.json')

# Write to file
with open(output_path, "w") as f:
    for event in events:
        json.dump(event, f)
        f.write("\n")

print(f"Generated file: {output_path.resolve()}")

# COMMAND ----------

import json
import base64
import random
from datetime import datetime
from pathlib import Path

def create_valid_event(i: int) -> dict:
    value_payload = {
        "user_id": f"user_{random.randint(1000, 9999)}",
        "event_type": random.choice(["click", "purchase", "view"]),
        "event_timestamp": datetime.now().isoformat(),
        "items": [
            {
                "item_id": f"item_{random.randint(100, 999)}",
                "quantity": random.randint(1, 5),
                "price_usd": round(random.uniform(10.0, 100.0), 2)
            }
            for _ in range(random.randint(1, 3))
        ]
    }
    value_json = json.dumps(value_payload)
    value_base64 = base64.b64encode(value_json.encode('utf-8')).decode('utf-8')

    return {
        "key": f"event_{i}",
        "timestamp": datetime.now().timestamp(),  # correct timestamp
        "value": value_base64
    }

def create_malformed_event(i: int) -> dict:
    value_payload = {
        "user_id": f"user_{random.randint(1000, 9999)}",
        "event_type": "malformed_event",
        "event_timestamp": datetime.now().isoformat(),
        "items": []
    }
    value_json = json.dumps(value_payload)
    value_base64 = base64.b64encode(value_json.encode('utf-8')).decode('utf-8')

    return {
        "key": f"event_{i}",
        "timestamp": "ERROR",  # <-- invalid timestamp
        "value": value_base64
    }

# Create 99 valid events
events = [create_valid_event(i) for i in range(99)]

# Add 1 malformed event at a random position
malformed_event = create_malformed_event(99)
insert_position = random.randint(0, 99)
events.insert(insert_position, malformed_event)


username_cleaned = DA.username.replace('.', '_')
# Output file path
output_path = Path(f'/Volumes/dbacademy/ops/{username_cleaned}/json_demo_files/lab_kafka_events_challenge.json')

with open(output_path, "w") as f:
    for event in events:
        json.dump(event, f)
        f.write("\n")

print(f"File written with 1 malformed 'timestamp' at position {insert_position}: {output_path.resolve()}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create solution table
# MAGIC CREATE OR REPLACE TABLE lab7_lab_kafka_events_raw
# MAGIC AS
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   cast(unbase64(value) as STRING) as decoded_value
# MAGIC FROM read_files(
# MAGIC         DA.paths_working_dir || '/json_demo_files/lab_kafka_events.json',
# MAGIC         format => "json", 
# MAGIC         schema => '''
# MAGIC           key STRING, 
# MAGIC           timestamp DOUBLE, 
# MAGIC           value STRING
# MAGIC         ''',
# MAGIC         rescueddatacolumn => '_rescued_data'
# MAGIC       );
# MAGIC
# MAGIC
# MAGIC -- Create the solution table with the correct data types
# MAGIC DROP TABLE IF EXISTS lab7_lab_kafka_events_flattened_solution;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE lab7_lab_kafka_events_flattened_solution
# MAGIC AS
# MAGIC SELECT 
# MAGIC   key,
# MAGIC   timestamp,
# MAGIC   decoded_value:user_id,
# MAGIC   decoded_value:event_type,
# MAGIC   cast(decoded_value:event_timestamp AS TIMESTAMP),
# MAGIC   from_json(decoded_value:items,'ARRAY<STRUCT<item_id: STRING, price_usd: DOUBLE, quantity: BIGINT>>') AS items
# MAGIC FROM lab7_lab_kafka_events_raw;
# MAGIC
# MAGIC
# MAGIC DROP TABLE IF EXISTS lab7_lab_kafka_events_raw;

# COMMAND ----------

# # This builds the final table so students can view how their table should look during the lab. 

# # Build v1_lab_kafka_events: Decode base64 and capture rescued data
# query_1 = f"""
# SELECT
#   * EXCEPT (_rescued_data),
#   CAST(unbase64(value) AS STRING) AS decoded_value,
#   _rescued_data
# FROM READ_FILES(
#   CONCAT('{DA.paths.working_dir}', '/json_demo_files/lab_kafka_events.json'),
#   FORMAT => 'json',
#   schemaHints => 'key STRING, timestamp STRING, value STRING, _rescued_data STRING'
# )
# """

# # Build v2_lab_kafka_events: Parse the decoded_value as STRUCT
# query_2 = f"""
# SELECT 
# CAST(key as STRING),
# CAST(timestamp as DOUBLE),
#   FROM_JSON(
#     decoded_value,
#     'STRUCT<
#       event_timestamp: STRING,
#       event_type: STRING,
#       items: ARRAY<STRUCT<
#         item_id: STRING,
#         price_usd: DOUBLE,
#         quantity: BIGINT
#       >>,
#       user_id: STRING
#     >'
#   ) AS value,
#   _rescued_data
# FROM (
#   {query_1}
# )
# """

# # Drop the table 07_lab_kafka_events if it exists
# query_3 = """
# DROP TABLE IF EXISTS 07_lab_solution;
# """

# # Create table 07_lab_kafka_events using query_2
# query_4 = f"""
# CREATE TABLE 07_lab_solution AS
# SELECT
#   key,
#   timestamp,
#   value.event_timestamp AS event_timestamp,
#   value.event_type AS event_type,
#   GET(value.items, 0).item_id AS item_id,
#   GET(value.items, 0).price_usd AS price_usd,
#   GET(value.items, 0).quantity AS quantity,
#   value.user_id AS user_id
# FROM (
#     {query_2}
# )
# """

# # Execute the queries
# spark.sql(query_3)
# spark.sql(query_4)

# COMMAND ----------

# # This builds the challenge table for students in case they need assistance. 

# query_5 = """
# DROP TABLE IF EXISTS 07_lab_challenge_solution;
# """

# query_6 = f"""
# CREATE TABLE 07_lab_challenge_solution AS 
# SELECT 
# * EXCEPT (timestamp),
# COALESCE(
#   timestamp,
#   case when from_json(_rescued_data, 'timestamp STRING')['timestamp'] = "ERROR" then 1759180800.0 else null end
# ) as timestamp_fixed 
# FROM READ_FILES(
#         CONCAT('{DA.paths.working_dir}', '/json_demo_files/lab_kafka_events.json'),
#         FORMAT => "json",
#         schemaHints => 'key STRING, timestamp DOUBLE, value STRING'
#       );
# """

# # Execute the queries
# spark.sql(query_5)
# spark.sql(query_6)