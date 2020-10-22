# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Summary and Next Steps
# MAGIC Congratulations!
# MAGIC You have completed Delta Lake Rapid Start with Python.
# MAGIC At this point, we invite you to think about the work we have done and how it relates to the full IoT data ingestion pipeline we have been designing.
# MAGIC In this course, we used Spark SQL and Delta Lake to do the following to create a Single Source of Truth in our EDSS, the health_tracker_processed Delta table.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We did this through the following steps:
# MAGIC - We converted an existing Parquet-based data lake table to a Delta table, health_tracker_processed.
# MAGIC - We performed a batch upload of new data to this table.
# MAGIC - We used Apache Spark to identify broken and missing records in this table.
# MAGIC - We used Delta Lake’s ability to do an upsert, where we updated broken records and inserted missing records.
# MAGIC - We evolved the schema of the Delta table.
# MAGIC - We used Delta Lake’s Time Travel feature to scrub the personal data of a user intelligently.
# MAGIC Additionally, we used Delta Lake to create an aggregate table, health_tracker_user_analytics, downstream from the health_tracker_processed table.
# MAGIC This concludes the Delta Lake Rapid Start with Python course.
