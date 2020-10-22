# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # The Big Picture
# MAGIC Building an EDSS with Delta Lake
# MAGIC In the Databricks Academy course Fundamentals of Delta Lake,
# MAGIC we discussed Enterprise Decision Support Systems (EDSS) and their use in Online Analytics Processing (OLAP).
# MAGIC In particular, we reviewed how using Delta Lake technology can help build robust
# MAGIC Cloud Data Platforms and single sources of truth to help organizations make intelligent, data-driven business decisions.
# MAGIC In this course, we go from theory to practice.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## An Internet of Things Data Ingestion Pipeline
# MAGIC In this course, we will use Apache Spark and Delta Lake to:
# MAGIC - Ingest data
# MAGIC - Create a table that will serve as a single source of truth
# MAGIC - Build a downstream aggregate table on this single source of truth
# MAGIC - Use Delta Lake to perform operations  on the table to make our data more robust

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The domain of this course is the Internet of Things (IoT).
# MAGIC In particular, we will be using simulated health tracker data passing
# MAGIC easurements of a user’s heart rate once an hour. Such a pipeline might appear as follows:

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ![Big Picture OLTP OLAP](./includes/images/01_big_pic_01-oltp_olap.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - High Flux Event Data - Heart rates are recorded by user devices.
# MAGIC - Operational Data Store (ODS) - The transactional database records all measurements.
# MAGIC - ETL - On some timeframe, this data is made available to an ETL process for loading into an EDSS.
# MAGIC - Enterprise Decision Support System (EDSS) - Data is loaded into a Delta table and is the Single Source of Truth.
# MAGIC - Online Analytics Processing (OLAP) - Delta Lake is used to manage and analyze the Single Source of Truth and downstream aggregate tables.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Next, we'll review how to configure your Databricks Workspace to perform these workflows.
