# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Create Delta Tables
# MAGIC Delta Table Fundamentals
# MAGIC Recall that a Delta table consists of three things:
# MAGIC - the data files kept in object storage (AWS S3, Azure Data Lake Storage)
# MAGIC - the Delta Transaction Log saved with the data files in object storage
# MAGIC - a table registered in the Metastore. This step is optional

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC When creating a Delta table, you can do one of the following:
# MAGIC - convert parquet files using the Delta Lake API
# MAGIC - write new files using the Spark DataFrame writer with .format("delta")
# MAGIC Either of these will automatically create the Transaction Log in the same top-level directory as the files. Optionally, you can register the table in the Metastore.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Creating a Table
# MAGIC Creating a table is one of the most fundamental actions performed when working with Delta Lake. With Delta Lake, you create tables:
# MAGIC when ingesting new files into a Delta Table for the first time
# MAGIC by transforming an existing Parquet-based data lake table to a Delta table

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC NOTE
# MAGIC Throughout this section, we'll be writing files to the root location of the Databricks File System (DBFS).
# MAGIC In general, best practice is to write files to your cloud object storage.
# MAGIC  We use DBFS root here for demonstration purposes.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 1: Describe the health_tracker_processed Table
# MAGIC Before we convert the health_tracker_processed table, let's use the DESCRIBE DETAIL Spark SQL command to display the attributes of the table.
# MAGIC Note that the table has format PARQUET.

# COMMAND ----------


%sql

DESCRIBE DETAIL health_tracker_processed

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Convert an Existing Parquet Table to a Delta table
# MAGIC When working with Delta Lake on Databricks,
# MAGIC Parquet files can be converted in-place to Delta files.
# MAGIC Next, we will convert the Parquet-based data lake table we created previously into a Delta table.
# MAGIC In doing so, we are defining the Single Source of Truth at the heart of our EDSS.
# MAGIC The health_tracker_processed will be the Single Source of Truth.
# MAGIC ![OLAP](./includes/images/03_delta_tables-01_olap.jpeg)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Step 1: Convert the Files to Delta Files
# MAGIC First, we'll convert the files in place to Parquet files.
# MAGIC The conversion creates a Delta Lake transaction log that tracks the files. Now, the directory is a directory of Delta files.

# COMMAND ----------


from delta.tables import DeltaTable

parquet_table = f"parquet.`{health_tracker}processed`"
partitioning_scheme = "p_device_id int"

DeltaTable.convertToDelta(spark, parquet_table, partitioning_scheme)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 2: Register the Delta Table
# MAGIC At this point, the files containing our records have been converted to Delta files.
# MAGIC The Metastore, however, has not been updated to reflect the change.
# MAGIC To change this we re-register the table in the Metastore.
# MAGIC The Spark SQL command will automatically infer the data schema by reading the footers of the Delta files.

# COMMAND ----------

%sql

DROP TABLE IF EXISTS health_tracker_processed;

CREATE TABLE health_tracker_processed
USING DELTA
LOCATION "/dbacademy/$username/DLRS/healthtracker/processed"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 3: Describe the health_tracker_processed Table
# MAGIC We can verify the conversion of the Parquet-based data lake table to a Delta table using the DESCRIBE Spark SQL command. Note that the format of the table is Delta.

# COMMAND ----------

%sql

DESCRIBE DETAIL health_tracker_processed


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 4: Count the Records in the health_tracker_processed table
# MAGIC We count the records in the health_tracker_processed table with Apache Spark.
# MAGIC With Delta Lake, the Delta table is immediately ready for use.

# COMMAND ----------

health_tracker_processed = spark.read.table("health_tracker_processed")
health_tracker_processed.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a New Delta Table
# MAGIC Next, we'll create a new Delta table. We'll do this by creating an aggregate table
# MAGIC from the data in the health_track_processed Delta table we just created.
# MAGIC Within the context of our EDSS, this is a downstream aggregate table or data mart.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 1: Remove files in the health_tracker_user_analytics directory
# MAGIC This step will make the notebook idempotent. In other words, it could be run more than once without throwing errors or introducing extra files.

# COMMAND ----------

dbutils.fs.rm(health_tracker + "gold/health_tracker_user_analytics",
              recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 2: Create an Aggregate DataFrame
# MAGIC The subquery used to define the table is an aggregate query over the health_tracker_processed Delta table using summary statistics for each device.

# COMMAND ----------


from pyspark.sql.functions import col, avg, max, stddev

health_tracker_gold_user_analytics = (
  health_tracker_processed
  .groupby("p_device_id")
  .agg(avg(col("heartrate")).alias("avg_heartrate"),
       max(col("heartrate")).alias("max_heartrate"),
       stddev(col("heartrate")).alias("stddev_heartrate"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 3: Write the Delta Files

# COMMAND ----------

(health_tracker_gold_user_analytics.write
 .format("delta")
 .mode("overwrite")
 .save(health_tracker + "gold/health_tracker_user_analytics"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 4: Register the Delta table in the Metastore
# MAGIC Finally, register this table in the Metastore.

# COMMAND ----------


# MAGIC %sql

DROP TABLE IF EXISTS health_tracker_gold_user_analytics;

CREATE TABLE health_tracker_gold_user_analytics
USING DELTA
LOCATION "/dbacademy/$username/DLRS/healthtracker/gold/health_tracker_user_analytics"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC _NOTE_
# MAGIC At Delta table creation, the Delta files in Object Storage define the schema, partitioning, and table properties. For this reason, it is not necessary to specify any of these when registering the table with the Metastore. Furthermore, NO TABLE REPAIR IS REQUIRED. The transaction log stored with the Delta files contains all metadata needed for an immediate query.
# MAGIC Prepare a Dashboard using the health_tracker_user_analytics Table
# MAGIC The health_tracker_user_analytics table could be used to define a dashboard. The query used to create the table could be issued against the health_tracker_processed table nightly to prepare the dashboard for the following business day.
# MAGIC Here we use Databricks’ built-in ability to create visualizations to create a basic dashboard.

# COMMAND ----------

display(spark.read.table("health_tracker_gold_user_analytics"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Configuring the Visualization
# MAGIC Note that we have used a Databricks visualization to view the aggregate sensor data.
# MAGIC We have used the following options to configure the visualization:
# MAGIC ```
# MAGIC Keys: p_device_id
# MAGIC Series groupings: None
# MAGIC Values: max_heartrate, avg_heartrate, stddev_heartrate
# MAGIC Aggregation: SUM
# MAGIC Display Type: Bar Chart
# MAGIC ```
