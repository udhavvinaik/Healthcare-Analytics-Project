# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Claim Enriched
# MAGIC
# MAGIC Purpose:
# MAGIC Enrich claims using beneficiary demographic
# MAGIC and provider fraud information.
# MAGIC
# MAGIC Business Objective:
# MAGIC - Connect claims with patients
# MAGIC - Connect claims with fraud labels
# MAGIC - Create analytical features
# MAGIC - Prepare data for Gold layer KPIs
# MAGIC
# MAGIC Input Tables:
# MAGIC - healthcare.silver.claims
# MAGIC - healthcare.silver.beneficiary
# MAGIC - healthcare.silver.fraudlabels
# MAGIC
# MAGIC Output Table:
# MAGIC - healthcare.silver.claim_enriched
# MAGIC ``

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

claims_df = spark.table(
    "healthcare.silver.claims"
)

beneficiary_df = spark.table(
    "healthcare.silver.beneficiary"
)

fraudlabels_df = spark.table(
    "healthcare.silver.fraudlabels"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Join claim transactions with beneficiary information.
# MAGIC
# MAGIC # Join Key:
# MAGIC  BeneID
# MAGIC
# MAGIC  Each claim now contains patient demographics,
# MAGIC  reimbursement indicators and chronic condition details.

# COMMAND ----------

claims_df.count()


# COMMAND ----------

beneficiary_df.count()

# COMMAND ----------

claims_beneficiary = (
    claims_df.join(
        beneficiary_df,
        "beneid",
        "left"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Join fraud labels.
# MAGIC
# MAGIC # Join Key:
# MAGIC  Provider
# MAGIC
# MAGIC  Adds fraud status for provider-level analytics.

# COMMAND ----------

claim_enriched = (
    claims_beneficiary.join(
        fraudlabels_df,
        "provider",
        "left"
    )
)

# COMMAND ----------

claim_enriched = (
    claim_enriched
    .withColumn(
        "fraudflag",
        when(
            col("potentialfraud")=="Yes",
            1
        ).otherwise(0)
    )
)

# COMMAND ----------

claim_enriched.printSchema()

# COMMAND ----------

claim_enriched.select("totalclaimcost").summary().show()

# COMMAND ----------

claim_enriched = (
    claim_enriched
    .withColumn(
        "claimvalueband",
        when(col("totalclaimcost").isNull(), "Unknown")
        .when(col("totalclaimcost") > 13068, "High")
        .when(col("totalclaimcost") > 5068, "Medium")
        .otherwise("Low")
    )
)

# COMMAND ----------

claim_enriched.select("chroniccount").summary().show()

# COMMAND ----------

claim_enriched = (
    claim_enriched
    .withColumn(
        "patientrisksegment",
        when(col("chroniccount") >= 9,"Very High")
        .when(col("chroniccount") >= 6,"High")
        .when(col("chroniccount") >= 3,"Medium")
        .otherwise("Low")
    )
)

# COMMAND ----------

claim_enriched = (
    claim_enriched
    .withColumn(
        "enriched_load_ts",
        current_timestamp()
    )
)

# COMMAND ----------

claim_enriched.printSchema()

# COMMAND ----------

display(
    claim_enriched.groupBy(
        "PotentialFraud"
    ).count()
)

# COMMAND ----------

display(
    claim_enriched.limit(10)
)

# COMMAND ----------

claim_enriched.printSchema()

# COMMAND ----------

claim_enriched = claim_enriched.toDF(*[col.lower().replace("_", "") for col in claim_enriched.columns])

# COMMAND ----------

print(f"Rows: {claim_enriched.count()}")
print(f"Columns: {len(claim_enriched.columns)}")

# COMMAND ----------

seen = {}
new_cols = []
for c in claim_enriched.columns:
    if c in seen:
        seen[c] += 1
        new_cols.append(f"{c}_{seen[c]}")
    else:
        seen[c] = 0
        new_cols.append(c)

(
    claim_enriched.toDF(*new_cols)
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "healthcare.silver.claim_enriched"
    )
)


# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE healthcare.silver.claim_enriched

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from healthcare.silver.claim_enriched