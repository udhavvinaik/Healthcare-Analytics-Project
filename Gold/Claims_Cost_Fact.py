# Databricks notebook source
from pyspark.sql.functions import *
CATALOG = "healthcare"

SOURCE_TABLE = f"{CATALOG}.silver.claim_enriched"
TARGET_TABLE = f"{CATALOG}.gold.fact_claims_cost"

# COMMAND ----------

claim_enriched_df = spark.table(SOURCE_TABLE)
claim_enriched_df.printSchema()

# COMMAND ----------

fact_claims_cost_df = claim_enriched_df.select(
    col("claimid").alias("ClaimID"),
    col("beneid").alias("BeneficiaryID"),
    col("provider").alias("ProviderID"),

    col("claimtype").alias("ClaimType"),

    col("claimstartdt").alias("ClaimStartDt"),
    col("claimenddt").alias("ClaimEndDt"),

    col("admissiondt").alias("AdmissionDt"),
    col("dischargedt").alias("DischargeDt"),

    col("claimdurationdays").alias("ClaimDurationDays"),
    col("losdays").alias("LengthOfStayDays"),

    col("inscclaimamtreimbursed").alias("ReimbursementAmount"),
    col("deductibleamtpaid").alias("DeductibleAmount"),
    col("totalclaimcost").alias("TotalClaimCost"),

    col("diagcount").alias("DiagnosisCount"),
    col("proccount").alias("ProcedureCount"),

    col("claimyear").alias("ClaimYear"),
    col("claimmonth").alias("ClaimMonth"),

    quarter(col("claimstartdt")).alias("ClaimQuarter"),

    date_format(
        col("claimstartdt"),
        "yyyy-MM"
    ).alias("ClaimYearMonth"),

    col("state").alias("State"),
    col("agegroup").alias("AgeGroup"),

    col("potentialfraud").alias("PotentialFraud")
)

# COMMAND ----------

(
    fact_claims_cost_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------

print(f"Created {TARGET_TABLE}")
display(fact_claims_cost_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from healthcare.gold.fact_claims_cost
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from healthcare.gold.fact_claims_cost 
# MAGIC where ClaimType = "Outpatient"