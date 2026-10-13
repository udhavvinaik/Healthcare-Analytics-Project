# Databricks notebook source
claim_en = spark.table("healthcare.silver.claim_enriched")
display(claim_en)


# COMMAND ----------

claim_en.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F

patient_360 = (
    claim_en.groupBy("beneid")
    .agg(

        F.first("gender").alias("gender"),
        F.first("race").alias("race"),
        F.first("state").alias("state"),
        F.first("county").alias("county"),

        F.first("age").alias("age"),
        F.first("isdeceased").alias("isdeceased"),

        F.first("chroniccondalzheimer").alias("chroniccondalzheimer"),
        F.first("chroniccondheartfailure").alias("chroniccondheartfailure"),
        F.first("chroniccondkidneydisease").alias("chroniccondkidneydisease"),
        F.first("chroniccondcancer").alias("chroniccondcancer"),
        F.first("chroniccondobstrpulmonary").alias("chroniccondobstrpulmonary"),
        F.first("chronicconddepression").alias("chronicconddepression"),
        F.first("chronicconddiabetes").alias("chronicconddiabetes"),
        F.first("chroniccondischemicheart").alias("chroniccondischemicheart"),
        F.first("chroniccondosteoporasis").alias("chroniccondosteoporasis"),
        F.first("chroniccondrheumatoidarthritis").alias("chroniccondrheumatoidarthritis"),
        F.first("chroniccondstroke").alias("chroniccondstroke"),

        F.first("chroniccount").alias("chronic_count"),

        F.first("ipannualreimbursementamt").alias("ipannualreimbursementamt"),
        F.first("ipannualdeductibleamt").alias("ipannualdeductibleamt"),
        F.first("opannualreimbursementamt").alias("opannualreimbursementamt"),
        F.first("opannualdeductibleamt").alias("opannualdeductibleamt"),

        F.countDistinct("claimid").alias("total_claims"),

        F.countDistinct(
            F.when(F.col("claimtype") == "Inpatient", F.col("claimid"))
        ).alias("ip_claims"),

        F.countDistinct(
            F.when(F.col("claimtype") == "Outpatient", F.col("claimid"))
        ).alias("op_claims"),

        F.sum("totalclaimcost").alias("total_paid"),

        F.countDistinct("provider").alias("distinct_providers"),

        F.max("claimstartdt").alias("last_claim_dt"),

        F.first("coveragegapmonth").alias("coverage_gap_months")
    )
)

# COMMAND ----------

patient_360.display()

# COMMAND ----------

patient_360.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true")\
    .saveAsTable("healthcare.gold.patient_360")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from healthcare.gold.patient_360;

# COMMAND ----------

# MAGIC %sql
# MAGIC show grant on healthcare.gold.patient_360