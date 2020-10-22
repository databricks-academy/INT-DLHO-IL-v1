# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Reviewing and Visualizing data
# MAGIC Review health tracker data
# MAGIC One common use case for working with Delta Lake is to collect and process Internet of Things (IoT) Data.
# MAGIC Here, we provide a mock IoT sensor dataset for demonstration purposes.
# MAGIC The data simulates heart rate data measured by a health tracker device.

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

# MAGIC %md ## Transactions
# MAGIC In this notebook, we will focus on the **transactions** piece of the pipeline.
# MAGIC
# MAGIC <img
# MAGIC      alt="Transactions"
# MAGIC      src=https://files.training.databricks.com/images/delta-lake-hands-on/01_big_pic_02-sst.jpeg
# MAGIC      width=600px
# MAGIC >
# MAGIC
# MAGIC In a typical system, high flux event data will be delivered to the system
# MAGIC via a stream processing server like Apache Kafka. For educational purposes,
# MAGIC we have made this data available for download from static files.
# MAGIC The commands we ran in the ingest raw notebook are used to download the data into our system and are
# MAGIC intended to simulate the arrival of high flux event data.
# MAGIC Here, we will simulate the streaming of data that is normally done by a
# MAGIC stream processing platform like Apache Kafka by accessing files from the raw directory.
# MAGIC These files are multi-line JSON files and resemble the strings passed by Kafka.
# MAGIC A multi-line JSON file is one in which each line is a complete JSON object,
# MAGIC but the entire file itself is not a valid JSON file.
# MAGIC Each file consists of five users whose heart rate is measured each hour, 24 hours a day, every day.
# MAGIC Here is a sample of the data we will be using.
# MAGIC Each line is a string representing a valid JSON object and is similar to the kind of string
# MAGIC that would be passed by a Kafka stream processing server.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Health tracker data sample
# MAGIC
# MAGIC ```
# MAGIC {"device_id":0,"heartrate":52.8139067501,"name":"Deborah Powell","time":1.5778368E9}
# MAGIC {"device_id":0,"heartrate":53.9078900098,"name":"Deborah Powell","time":1.5778404E9}
# MAGIC {"device_id":0,"heartrate":52.7129593616,"name":"Deborah Powell","time":1.577844E9}
# MAGIC {"device_id":0,"heartrate":52.2880422685,"name":"Deborah Powell","time":1.5778476E9}
# MAGIC {"device_id":0,"heartrate":52.5156095386,"name":"Deborah Powell","time":1.5778512E9}
# MAGIC {"device_id":0,"heartrate":53.6280743846,"name":"Deborah Powell","time":1.5778548E9}
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
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Load the Data
# MAGIC Load the data as a Spark DataFrame from the raw directory.
# MAGIC This is done using the `.format("json")` option.

# COMMAND ----------


file_path = health_tracker + "raw/health_tracker_data_2020_1.json"

health_tracker_data_2020_1_df = (
  spark.read
  .format("json")
  .load(file_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Visualize Data
# MAGIC ### Step 1: Display the Data
# MAGIC Strictly speaking, this is not part of the ETL process, but displaying the data gives us a look at the data that we are working with.
# MAGIC We note a few phenomena in the data:
# MAGIC - Sensor anomalies - Sensors cannot record negative heart rates, so any negative values in the data are anomalies.
# MAGIC - Wake/Sleep cycle - We notice that users have a consistent wake/sleep cycle alternating between steady high and low heart rates.
# MAGIC - Elevated activity - Some users have irregular periods of high activity.

# COMMAND ----------

display(health_tracker_data_2020_1_df)

# COMMAND ----------


# MAGIC %md
# MAGIC
# MAGIC ### Configuring the Visualization
# MAGIC Create a Databricks visualization to visualize the sensor data over time. We have used the following options to configure the visualization:
# MAGIC ```
# MAGIC Keys: time
# MAGIC Series groupings: device_id
# MAGIC Values: heartrate
# MAGIC Aggregation: SUM
# MAGIC Display Type: Bar Chart
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step 2: Configure the Visualization
# MAGIC Create a Databricks visualization to visualize the sensor data over time.
# MAGIC We have used the following plot options to configure the visualization:
# MAGIC ```
# MAGIC Keys: time
# MAGIC Series groupings: device_id
# MAGIC Values: heartrate
# MAGIC Aggregation: SUM
# MAGIC Display Type: Bar Chart
# MAGIC ```
# MAGIC Now that we have a better idea of the data we're working with, let's move on to create a Parquet-based table from this data.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Create a Parquet Table
# MAGIC Now that we have used Databricks to preview the data, we'll work through the process of creating a Parquet-based data lake table. This table will be used in the next lesson to show the ease of converting existing Parquet-based tables to Delta tables.
# MAGIC The development pattern used to create a Parquet-based data lake table is similar to that used in creating a Delta table. There are a few issues that arise as part of the process, however. In particular, working with Parquet-based tables often requires table repairs to work with them.
# MAGIC In subsequent lessons, we'll see that creating a Delta table does not have the same issues.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 1: Make Idempotent
# MAGIC First, we remove the files in the `healthtracker/processed` directory.
# MAGIC
# MAGIC Then, we drop the table we will create from the Metastore if it exists.
# MAGIC
# MAGIC This step will make the notebook idempotent. In other words, it could be run more than once without throwing errors or introducing extra files.
# MAGIC
# MAGIC 🚨 **NOTE** Throughout this lesson, we'll be writing files to the root location of the Databricks File System (DBFS). In general, best practice is to write files to your cloud object storage. We use DBFS root here for demonstration purposes.

# COMMAND ----------

dbutils.fs.rm(health_tracker + "processed", recurse=True)

spark.sql(f"""
DROP TABLE IF EXISTS health_tracker_processed
""")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 2: Transform the Data
# MAGIC We perform data engineering on the data with the following transformations:
# MAGIC - Use the `from_unixtime` Spark SQL function to transform the unixtime into a time string
# MAGIC - Cast the time column to type `timestamp` to replace the column `time`
# MAGIC - Cast the time column to type `date` to create the column `dte`
# MAGIC - Select the columns in the order in which we would like them to be written

# COMMAND ----------

from pyspark.sql.functions import col, from_unixtime

def process_health_tracker_data(dataframe):
  return (
    dataframe
    .withColumn("time", from_unixtime("time"))
    .withColumnRenamed("device_id", "p_device_id")
    .withColumn("time", col("time").cast("timestamp"))
    .withColumn("dte", col("time").cast("date"))
    .withColumn("p_device_id", col("p_device_id").cast("integer"))
    .select("dte", "time", "heartrate", "name", "p_device_id")
    )

processedDF = process_health_tracker_data(health_tracker_data_2020_1_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 3: Write the Files to the processed directory
# MAGIC Note that we are partitioning the data by device id.

# COMMAND ----------

(processedDF.write
 .mode("overwrite")
 .format("parquet")
 .partitionBy("p_device_id")
 .save(health_tracker + "processed"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 4: Register the Table in the Metastore
# MAGIC Next, use Spark SQL to register the table in the metastore.
# MAGIC Upon creation we specify the format as parquet and that the location where the parquet files were written should be used.

# COMMAND ----------

spark.sql(f"""
DROP TABLE IF EXISTS health_tracker_processed
""")

spark.sql(f"""
CREATE TABLE health_tracker_processed
USING PARQUET
LOCATION "{health_tracker}/processed"
""")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 5: Verify and Repair the Parquet-based Data Lake table
# MAGIC #### Step 5a: Count the Records in the health_tracker_processed Table
# MAGIC Per best practice, we have created a partitioned table.
# MAGIC However, if you create a partitioned table from existing data,
# MAGIC Spark SQL does not automatically discover the partitions and register them in the Metastore.
# MAGIC Note that the count does not return results.

# COMMAND ----------

health_tracker_processed = spark.read.table("health_tracker_processed")
health_tracker_processed.count()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 5b: Register the Partitions
# MAGIC To register the partitions, run the following to generate the partitions:

# COMMAND ----------

spark.sql("MSCK REPAIR TABLE health_tracker_processed")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 5c: Count the Records in the health_tracker_processed Table
# MAGIC Count the records in the health_tracker_processed table.
# MAGIC With the table repaired and the partitions registered, we now have results.
# MAGIC We expect there to be 3720 records: five device measurements, 24 hours a day for 31 days.

# COMMAND ----------

health_tracker_processed.count()
