# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Batch Write to Delta Tables
# MAGIC Appending Files to an Existing Delta Table
# MAGIC There are two patterns for modifying existing Delta tables:
# MAGIC appending files to an existing directory of Delta files
# MAGIC merging a set of updates and insertions
# MAGIC In this lesson, we explore the first.
# MAGIC Within the context of our data ingestion pipeline, this is the addition of new raw files to our Single Source of Truth.
# MAGIC Step 1: Load the Next Month of Data
# MAGIC Here, we append the next month of records. We begin by loading the data from the file health_tracker_data_2020_2.json, using the .format("json") option as before.

# COMMAND ----------

file_path = health_tracker + "raw/health_tracker_data_2020_2.json"

health_tracker_data_2020_2_df = (
  spark.read
  .format("json")
  .load(file_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 2: Transform the Data
# MAGIC We perform the same data engineering on the data:
# MAGIC - Use the from_unixtime Spark SQL function to transform the unixtime into a time string
# MAGIC - Cast the time column to type timestamp to replace the column time
# MAGIC - Cast the time column to type date to create the column dte

# COMMAND ----------

processedDF = process_health_tracker_data(health_tracker_data_2020_2_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 3: Append the Data to the health_tracker_processed Delta table
# MAGIC We do this using .mode("append"). Note that it is not necessary to perform any action on the Metastore.

# COMMAND ----------

(processedDF.write
 .mode("append")
 .format("delta")
 .save(health_tracker + "processed"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC View the Commit Using Time Travel
# MAGIC Delta Lake can query an earlier version of a Delta table using a feature known as time travel. Here, we query the data as of version 0, that is, the initial conversion of the table from Parquet.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 1: View the table as of Version 0
# MAGIC This is done by specifying the option "versionAsOf" as 0. When we time travel to Version 0, we see only the first month of data, five device measurements, 24 hours a day for 31 days.

# COMMAND ----------

(spark.read
 .option("versionAsOf", 0)
 .format("delta")
 .load(health_tracker + "processed")
 .count())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 2: Count the Most Recent Version
# MAGIC When we query the table without specifying a version, it shows the latest version of the table and includes the new records added.
# MAGIC When we look at the current version, we expect to see two months of data, five device measurements, 24 hours a day for (31 + 29) days, or 7200 records. Note that the range of data includes the month of February during a leap year. That is why there are 29 days in the month.
# MAGIC Note that we do not have a correct count. We are missing 72 records.

# COMMAND ----------

health_tracker_processed.count()
