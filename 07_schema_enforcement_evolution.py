# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Schema Enforcement & Evolution
# MAGIC ## Evolution of Data Being Ingested
# MAGIC It is not uncommon that the data being ingested into the EDSS will evolve over time. In this case, the simulated health tracker device has a new version available and the data being transmitted now indicates which type of device is being used.

# COMMAND ----------

# MAGIC %md ## Notebook Configuration
# MAGIC
# MAGIC Before you run this cell, make sure to add a unique user name to the file
# MAGIC `includes/configuration`, e.g.
# MAGIC
# MAGIC ```
# MAGIC username = "yourfirstname_yourlastname"
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Health tracker data sample
# MAGIC
# MAGIC ```
# MAGIC {"device_id":0,"heartrate":57.6447293596,"name":"Deborah Powell","time":1.5830208E9,"device_type":"version 2"}
# MAGIC {"device_id":0,"heartrate":57.6175546013,"name":"Deborah Powell","time":1.5830244E9,"device_type":"version 2"}
# MAGIC {"device_id":0,"heartrate":57.8486376876,"name":"Deborah Powell","time":1.583028E9,"device_type":"version 2"}
# MAGIC {"device_id":0,"heartrate":57.8821378637,"name":"Deborah Powell","time":1.5830316E9,"device_type":"version 2"}
# MAGIC {"device_id":0,"heartrate":59.0531490807,"name":"Deborah Powell","time":1.5830352E9,"device_type":"version 2"}
# MAGIC ```
# MAGIC This shows a sample of the health tracker data we will be using. Note that each line is a valid JSON object.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Health tracker data schema
# MAGIC The data has the following schema:
# MAGIC
# MAGIC ```
# MAGIC name: string
# MAGIC heartrate: double
# MAGIC device_id: long
# MAGIC time: long
# MAGIC device_type: string
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %md ### Appending Files to an Existing Delta Table
# MAGIC Our goal is to append the next month of data.
# MAGIC ### Step 1: Load the Next Month of Data
# MAGIC We begin by loading the data from the file health_tracker_data_2020_3.json, using the .format("json") option as before.

# COMMAND ----------

file_path = health_tracker + "raw/health_tracker_data_2020_3.json"


health_tracker_data_2020_3_df = (
  spark.read
  .format("json")
  .load(file_path)
)

# COMMAND ----------

# MAGIC %md ### Step 2: Transform the Data
# MAGIC We perform the same data engineering on the data:
# MAGIC - Use the from_unixtime Spark SQL function to transform the unixtime into a time string
# MAGIC - Cast the time column to type timestamp to replace the column time
# MAGIC - Cast the time column to type date to create the column dte

# COMMAND ----------

def process_health_tracker_data(dataframe):
  return (
    dataframe
    .withColumn("time", from_unixtime("time"))
    .withColumnRenamed("device_id", "p_device_id")
    .withColumn("time", col("time").cast("timestamp"))
    .withColumn("dte", col("time").cast("date"))
    .withColumn("p_device_id", col("p_device_id").cast("integer"))
    .select("dte", "time", "device_type", "heartrate", "name", "p_device_id")
    )
processedDF = process_health_tracker_data(health_tracker_data_2020_3_df)

# COMMAND ----------

# MAGIC %md ### Step 3: Append the Data to the health_tracker_processed Delta table
# MAGIC We do this using `.mode("append")`.

# COMMAND ----------

from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import lit

try:
  (
    processedDF.write
    .mode("append")
    .format("delta")
    .save(health_tracker + "processed")
  )
except AnalysisException as error:
  print("Analysis Exception:")
  print(error)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Schema Mismatch
# MAGIC When we try to run this command, we receive the error shown below because there is a mismatch between the table and data schemas.
# MAGIC `AnalysisException: A schema mismatch detected when writing to the Delta table (Table ID: ...).`
# MAGIC 
# MAGIC To enable schema migration using DataFrameWriter or DataStreamWriter, please set: '.option("mergeSchema", "true")'.
# MAGIC 
# MAGIC For other operations, set the session configuration spark.databricks.delta.schema.autoMerge.enabled to "true". See the documentation specific to the operation for details.


# COMMAND ----------

# MAGIC %md ## What Is Schema Enforcement?
# MAGIC Schema enforcement, also known as schema validation, is a safeguard in Delta Lake that ensures data quality by rejecting writes to a table that do not match the table’s schema. Like the front desk manager at a busy restaurant that only accepts reservations, it checks to see whether each column in data inserted into the table is on its list of expected columns (in other words, whether each one has a “reservation”), and rejects any writes with columns that aren’t on the list.
# MAGIC Appending Files to an Existing Delta Table with Schema Evolution
# MAGIC In this case, we would like our table to accept the new schema and add the data to the table.

# COMMAND ----------

# MAGIC %md ## What Is Schema Evolution?
# MAGIC Schema evolution is a feature that allows users to easily change a table’s current schema to accommodate data that is changing over time. Most commonly, it’s used when performing an append or overwrite operation, to automatically adapt the schema to include one or more new columns.
# MAGIC ### Step 1: Append the Data with Schema Evolution to the health_tracker_processed Delta table
# MAGIC We do this using .mode("append").

# COMMAND ----------

(processedDF.write
 .mode("append")
 .option("mergeSchema", True)
 .format("delta")
 .save(health_tracker + "processed"))


# COMMAND ----------

# MAGIC %md ## Verify the Commit
# MAGIC ### Step 1: Count the Most Recent Version
# MAGIC When we look at the current version, we expect to see three months of data, five device measurements, 24 hours a day for (31 + 29 + 31) days, or 10920 records. Note that the range of data includes the month of February during a leap year. That is why there are 29 days in the month.

# COMMAND ----------

health_tracker_processed.count()
