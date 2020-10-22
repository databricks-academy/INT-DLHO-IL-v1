# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Unit Tests for Operations

# COMMAND ----------

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# COMMAND ----------

from pyspark import sql

"""
For local testing it is necessary to instantiate the Spark Session in order to have 
Delta Libraries installed prior to import in the next cell
"""

spark = sql.SparkSession.builder.master("local[8]").getOrCreate()

# COMMAND ----------

from main.python.operations import transform_raw

# COMMAND ----------


@pytest.fixture(scope="session")
def spark_session(request):
    """Fixture for creating a spark context."""
    request.addfinalizer(lambda: spark.stop())

    return spark


# COMMAND ----------


def test_transform_raw(spark_session: SparkSession):
    testDF = spark_session.createDataFrame(
        [
            (
                '{"device_id":0,"heartrate":52.8139067501,"name":"Deborah Powell","time":1.5778368E9}',
            ),
            (
                '{"device_id":0,"heartrate":53.9078900098,"name":"Deborah Powell","time":1.5778404E9}',
            ),
            (
                '{"device_id":0,"heartrate":52.7129593616,"name":"Deborah Powell","time":1.577844E9}',
            ),
            (
                '{"device_id":0,"heartrate":52.2880422685,"name":"Deborah Powell","time":1.5778476E9}',
            ),
            (
                '{"device_id":0,"heartrate":52.5156095386,"name":"Deborah Powell","time":1.5778512E9}',
            ),
            (
                '{"device_id":0,"heartrate":53.6280743846,"name":"Deborah Powell","time":1.5778548E9}',
            ),
        ],
        schema="value STRING",
    )
    transformedDF = transform_raw(testDF)
    assert transformedDF.schema == StructType(
        [
            StructField("datasource", StringType(), False),
            StructField("ingesttime", TimestampType(), False),
            StructField("value", StringType(), True),
            StructField("p_ingestdate", DateType(), False),
        ]
    )
