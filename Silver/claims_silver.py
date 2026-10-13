# Databricks notebook source
# MAGIC %md
# MAGIC Purpose:
# MAGIC This notebook creates a unified claims dataset by combining inpatient and outpatient data.
# MAGIC  
# MAGIC
# MAGIC Business Objective:
# MAGIC - Create a single source of truth for claim transactions
# MAGIC - Standardize schemas across claim types
# MAGIC - Enable downstream fraud analytics
# MAGIC - Prepare data for Gold layer aggregations
# MAGIC  
# MAGIC
# MAGIC Input Tables:
# MAGIC - healthcare.silver.inpatientdata
# MAGIC - healthcare.silver.outpatientdata
# MAGIC  
# MAGIC
# MAGIC Output Table:
# MAGIC - healthcare.silver.claims

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

inpatient_df = spark.table("healthcare.silver.inpatientdata")

outpatient_df = spark.table("healthcare.silver.outpatientdata")

# COMMAND ----------

# MAGIC %md
# MAGIC #Standardize Inpatient Schema
# MAGIC
# MAGIC # Objective:
# MAGIC  Create a consistent structure that can be merged with
# MAGIC  outpatient claims.
# MAGIC
# MAGIC # Business Benefit:
# MAGIC  Enables unified reporting across all claim types.

# COMMAND ----------

inpatient_claims = inpatient_df


# COMMAND ----------

# MAGIC %md
# MAGIC #Standardize Outpatient Schema

# COMMAND ----------

outpatient_claims = outpatient_df

# COMMAND ----------

claims = inpatient_claims.unionByName(outpatient_claims,allowMissingColumns=True)

# COMMAND ----------

display(
    claims.groupBy("CLAIMTYPE")
                .count()
)

# COMMAND ----------

claims.printSchema()

# COMMAND ----------

claims.show()

# COMMAND ----------

claims = claims.toDF(*[col.lower().replace("_", "") for col in claims.columns])

# COMMAND ----------

# MAGIC %md
# MAGIC # high_cost_flag && claim_amt_zscore && duplicate_claim_flag && age_at_claim. 

# COMMAND ----------

claims.select('totalclaimcost').summary().show()

# COMMAND ----------

from pyspark.sql import functions as F

thresholds = claims.groupBy("claimtype").agg(
    F.percentile_approx("totalclaimcost", 0.85).alias("p85_cost")
)

claims = claims.join(thresholds, on="claimtype", how="left")

claims = claims.withColumn(
    "high_cost_flag",
    F.when(F.col("totalclaimcost") > F.col("p85_cost"), 1).otherwise(0)
)

# COMMAND ----------

stats = claims.groupBy("claimtype").agg(
    F.mean("totalclaimcost").alias("mean_cost"),
    F.stddev("totalclaimcost").alias("std_cost")
)

claims = claims.join(stats, "claimtype")

claims = claims.withColumn(
    "claim_amt_zscore",
    (
        F.col("totalclaimcost") - F.col("mean_cost")
    ) / F.col("std_cost")
)

# COMMAND ----------

from pyspark.sql import functions as F

temp_view = claims.groupBy(
    "beneid",
    "provider",
    "claimstartdt",
    "totalclaimcost"
).count() \
 .filter(F.col("count") > 1) \
 .orderBy(F.desc("count"))

# COMMAND ----------

from pyspark.sql import functions as F

display(
    temp_view.orderBy(
        # F.desc("totalclaimcost"),
        F.desc("provider")
    )
)

# COMMAND ----------

from pyspark.sql.window import Window

w = Window.partitionBy(
    "beneid",
    "provider",
    "claimstartdt",
    "totalclaimcost"
)

claims = claims.withColumn(
    "duplicate_claim_flag",
    F.when(F.count("*").over(w) > 1, 1).otherwise(0)
)

# COMMAND ----------

(
    claims.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("healthcare.silver.claims")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from healthcare.silver.claims where claimtype="Outpatient"