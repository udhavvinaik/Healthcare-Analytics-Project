# Databricks notebook source
from pyspark.sql.functions import *

(
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("cloudFiles.schemaLocation", "/Volumes/healthcare/bronze/raw_data/_schemas/beneficiary")
    .option("pathGlobFilter", "*Beneficiarydata*.csv")
    .load("/Volumes/healthcare/bronze/raw_data")
    .writeStream
    .outputMode("append")
    .option("checkpointLocation",
            "/Volumes/healthcare/bronze/raw_data/_checkpoints/beneficiary")
    .trigger(availableNow=True)
    .toTable("healthcare.bronze.beneficiary")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from healthcare.bronze.beneficiary;

# COMMAND ----------

from pyspark.sql.functions import *

(
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("cloudFiles.schemaLocation", "/Volumes/healthcare/bronze/raw_data/_schemas/inpatientdata")
    .option("pathGlobFilter", "*Inpatientdata*.csv")
    .load("/Volumes/healthcare/bronze/raw_data")
    .writeStream
    .outputMode("append")
    .option("checkpointLocation",
            "/Volumes/healthcare/bronze/raw_data/_checkpoints/inpatientdata")
    .trigger(availableNow=True)
    .toTable("healthcare.bronze.inpatientdata")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from healthcare.bronze.inpatientdata;

# COMMAND ----------

from pyspark.sql.functions import *

(
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("cloudFiles.schemaLocation", "/Volumes/healthcare/bronze/raw_data/_schemas/outpatientdata")
    .option("pathGlobFilter", "*Outpatientdata*.csv")
    .load("/Volumes/healthcare/bronze/raw_data")
    .writeStream
    .outputMode("append")
    .option("checkpointLocation",
            "/Volumes/healthcare/bronze/raw_data/_checkpoints/outpatientdata")
    .trigger(availableNow=True)
    .toTable("healthcare.bronze.outpatientdata")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from healthcare.bronze.outpatientdata;

# COMMAND ----------

from pyspark.sql.functions import *

(
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("cloudFiles.schemaLocation", "/Volumes/healthcare/bronze/raw_data/_schemas/fraudlabels")
    .option("pathGlobFilter", "*Fraudlabels*.csv")
    .load("/Volumes/healthcare/bronze/raw_data")
    .writeStream
    .outputMode("append")
    .option("checkpointLocation",
            "/Volumes/healthcare/bronze/raw_data/_checkpoints/fraudlabels")
    .trigger(availableNow=True)
    .toTable("healthcare.bronze.fraudlabels")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from healthcare.bronze.fraudlabels;

# COMMAND ----------

