# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import DateType
from pyspark.sql.window import Window
spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

CATALOG = "healthcare"
BRONZE = f"{CATALOG}.bronze.fraudlabels"
SILVER = f"{CATALOG}.silver.fraudlabels"

# COMMAND ----------

bronze_df = spark.table(BRONZE)
print(bronze_df.count())

# COMMAND ----------

#display(bronze_df).limit(10)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

fraud_df = spark.table("healthcare.bronze.fraudlabels")

provider_window = Window.partitionBy("Provider")

fraud_quarantine = (

    fraud_df

    .withColumn(
        "provider_record_count",
        count("*").over(provider_window)
    )

    .withColumn(

        "quarantine_reason",

        when(
            col("Provider").isNull(),
            "Missing Provider"
        )

        .when(
            col("PotentialFraud").isNull(),
            "Missing Fraud Label"
        )

        .when(
            ~col("PotentialFraud").isin("Yes","No"),
            "Invalid Fraud Label"
        )

        .otherwise(None)

    )

    .filter(
        col("quarantine_reason").isNotNull()
    )

    .withColumn(
        "quarantine_ts",
        current_timestamp()
    )

)

# COMMAND ----------

display(fraud_quarantine)

# COMMAND ----------

# MAGIC %md
# MAGIC #

# COMMAND ----------

# MAGIC %md
# MAGIC ENABLE CDF AND SAVE INTO SILVER

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC ALTER TABLE healthcare.bronze.fraudlabels
# MAGIC SET TBLPROPERTIES (
# MAGIC 'delta.enableChangeDataFeed'='true'
# MAGIC );

# COMMAND ----------

bronze_df = bronze_df.toDF(*[col.lower().replace("_", "") for col in bronze_df.columns])

# COMMAND ----------

bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("healthcare.silver.fraudlabels")

# COMMAND ----------

bronze_df.printSchema()