# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Raw Data Retrieval

# COMMAND ----------

# MAGIC %md ## Notebook Objective
# MAGIC
# MAGIC In this notebook we:
# MAGIC
# MAGIC 1. Ingest data from a remote source into our source directory, `rawPath`.

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

# MAGIC %md ## Make Course Idempotent

# COMMAND ----------

dbutils.fs.rm(health_tracker, recurse=True)

spark.sql(f"""
DROP TABLE IF EXISTS health_tracker_processed
""")

spark.sql(f"""
DROP TABLE IF EXISTS health_tracker_gold_aggregate_heartrate
""")

# COMMAND ----------

# MAGIC %md ## Retrieve First Month of Data
# MAGIC
# MAGIC Next, we use the utility function, `retrieve_data` to
# MAGIC retrieve the files we will ingest. The function takes
# MAGIC three arguments:
# MAGIC
# MAGIC - `year: int`
# MAGIC - `month: int`
# MAGIC - `rawPath: str`
# MAGIC - `is_late: bool` (optional)

# COMMAND ----------

retrieve_data(2020, 1, health_tracker + "raw/")
retrieve_data(2020, 2, health_tracker + "raw/")
retrieve_data(2020, 2, health_tracker + "raw/", is_late=True)
retrieve_data(2020, 3, health_tracker + "raw/")

# COMMAND ----------

# MAGIC %md ### Expected File
# MAGIC
# MAGIC The expected file has the following name:

# COMMAND ----------

file_2020_1 = "health_tracker_data_2020_1.json"

# COMMAND ----------

# MAGIC %md ### Display the Files in the Raw Path

# COMMAND ----------

display(dbutils.fs.ls(health_tracker + "raw/"))

# COMMAND ----------

# MAGIC %md **Exercise:** Write an Assertion Statement to Verify File Ingestion
# MAGIC
# MAGIC Note: the `print` statement would typically not be included in production code, nor in code used to test this notebook.

# COMMAND ----------

# TODO
# assert FILL_THIS_IN in [item.name for item in dbutils.fs.ls(FILL_THIS_IN)], "File not present in Raw Path"
# print("Assertion passed.")

# COMMAND ----------

# ANSWER
assert file_2020_1 in [
    item.name for item in dbutils.fs.ls(health_tracker + "raw/")
], "File not present in Raw Path"
print("Assertion passed.")
