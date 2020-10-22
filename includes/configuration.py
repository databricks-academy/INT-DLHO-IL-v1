# Databricks notebook source
# MAGIC %md Define Data Paths.

# COMMAND ----------

# TODO
# username = FILL_THIS_IN

# COMMAND ----------

# ANSWER
username = "dbacademy"

# COMMAND ----------

health_tracker = f"/dbacademy/{username}/delta-lake-hands-on/health-tracker/"

# COMMAND ----------

# MAGIC %md Configure Database

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS dbacademy_{username}")
spark.sql(f"USE dbacademy_{username}")

# COMMAND ----------

# MAGIC %md Import Utility Functions

# COMMAND ----------

# MAGIC %run ./utilities
