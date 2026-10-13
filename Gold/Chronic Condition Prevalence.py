# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

claim_en_df = spark.table("healthcare.silver.claim_enriched")
display(claim_en_df)


# COMMAND ----------

from pyspark.sql import functions as F

chronic_cols = [
    "chroniccondalzheimer",
    "chroniccondheartfailure",
    "chroniccondkidneydisease",
    "chroniccondcancer",
    "chroniccondobstrpulmonary",
    "chronicconddepression",
    "chronicconddiabetes",
    "chroniccondischemicheart",
    "chroniccondosteoporasis",
    "chroniccondrheumatoidarthritis",
    "chroniccondstroke"
]

# COMMAND ----------

patient_df = (
    claim_en_df
    .groupBy(
        "beneid",
        "state",
        "gender",
        "agegroup"
    )
    .agg(
        F.max("totalannualreimbursement").alias("totalannualreimbursement"),
        F.countDistinct("claimid").alias("claim_count"),

        *[
            F.max(F.col(c)).alias(c)
            for c in chronic_cols
        ]
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #Step 3: Convert Chronic Columns into Rows (Melt / Unpivot)

# COMMAND ----------

stack_expr = """
stack(
11,
'Diabetes', chronicconddiabetes,
'HeartFailure', chroniccondheartfailure,
'KidneyDisease', chroniccondkidneydisease,
'Cancer', chroniccondcancer,
'COPD', chroniccondobstrpulmonary,
'Depression', chronicconddepression,
'Alzheimer', chroniccondalzheimer,
'IschemicHeartDisease', chroniccondischemicheart,
'Osteoporosis', chroniccondosteoporasis,
'RheumatoidArthritis', chroniccondrheumatoidarthritis,
'Stroke', chroniccondstroke
) as (condition_name, condition_flag)
"""

condition_long_df = patient_df.select(
    "*",
    F.expr(stack_expr)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #Step 4: Keep Only Patients Having That Condition

# COMMAND ----------

condition_patients = condition_long_df.filter(
    F.col("condition_flag") == 1
)

# COMMAND ----------

# MAGIC %md
# MAGIC State Segmentation

# COMMAND ----------

state_prevalence = (
    condition_patients
    .groupBy(
        "condition_name",
        F.lit("State").alias("segment_type"),
        F.col("state").alias("segment_value")
    )
    .agg(
        F.countDistinct("beneid").alias("patients_with_condition"),
        F.avg("totalannualreimbursement").alias("avg_reimbursement"),
        F.round(
            F.sum("claim_count")
            / F.countDistinct("beneid"),0
        ).alias("avg_numberof_claims_per_patient")
    )
)

# COMMAND ----------

state_totals = (
    patient_df
    .groupBy("state")
    .agg(
        F.countDistinct("beneid").alias("total_patients")
    )
)

# COMMAND ----------

state_prevalence = (
    state_prevalence
    .join(
        state_totals,
        state_prevalence.segment_value == state_totals.state,
        "left"
    )
    .drop("state")
    .withColumn(
        "prevalence_pct",
        F.round(
            F.col("patients_with_condition")
            * 100.0
            / F.col("total_patients"),
            2
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Gender Segmentation

# COMMAND ----------

gender_prevalence = (
    condition_patients
    .groupBy(
        "condition_name",
        F.lit("Gender").alias("segment_type"),
        F.col("gender").cast("string").alias("segment_value")
    )
    .agg(
        F.countDistinct("beneid").alias("patients_with_condition"),
        F.avg("totalannualreimbursement").alias("avg_reimbursement"),
        (
            F.round(
                F.sum("claim_count")
                / F.countDistinct("beneid"),0
            )
        ).alias("avg_numberof_claims_per_patient")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Total Patients By Gender

# COMMAND ----------

gender_totals = (
    patient_df
    .groupBy("gender")
    .agg(
        F.countDistinct("beneid").alias("total_patients")
    )
)

# COMMAND ----------

gender_prevalence = (
    gender_prevalence
    .join(
        gender_totals,
        gender_prevalence.segment_value ==
        gender_totals.gender.cast("string"),
        "left"
    )
    .drop("gender")
    .withColumn(
        "prevalence_pct",
        F.round(
            F.col("patients_with_condition")
            * 100.0
            / F.col("total_patients"),
            2
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #Age Group Segmentation

# COMMAND ----------

age_prevalence = (
    condition_patients
    .groupBy(
        "condition_name",
        F.lit("AgeGroup").alias("segment_type"),
        F.col("agegroup").alias("segment_value")
    )
    .agg(
        F.countDistinct("beneid").alias("patients_with_condition"),
        F.avg("totalannualreimbursement").alias("avg_reimbursement"),
        F.round(
            F.sum("claim_count")
            / F.countDistinct("beneid"),0
        ).alias("avg_numberof_claims_per_patient")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Total Patients By Age Group

# COMMAND ----------

age_totals = (
    patient_df
    .groupBy("agegroup")
    .agg(
        F.countDistinct("beneid").alias("total_patients")
    )
)

# COMMAND ----------

age_prevalence = (
    age_prevalence
    .join(
        age_totals,
        age_prevalence.segment_value == age_totals.agegroup,
        "left"
    )
    .drop("agegroup")
    .withColumn(
        "prevalence_pct",
        F.round(
            F.col("patients_with_condition")
            * 100.0
            / F.col("total_patients"),
            2
        )
    )
)

# COMMAND ----------

chronic_condition_prevalence = (
    state_prevalence
    .unionByName(gender_prevalence)
    .unionByName(age_prevalence)
)

# COMMAND ----------

chronic_condition_prevalence.printSchema()

# COMMAND ----------

chronic_condition_prevalence.columns

# COMMAND ----------

chronic_condition_prevalence.dtypes

# COMMAND ----------

from pyspark.sql.types import *

for field in chronic_condition_prevalence.schema.fields:
    print(field.name, "---->", field.dataType)

# COMMAND ----------

# DBTITLE 1,Cell 28
chronic_condition_prevalence = (
    state_prevalence
    .withColumn("segment_value", F.col("segment_value").cast("string"))
    .unionByName(gender_prevalence.withColumn("segment_value", F.col("segment_value").cast("string")))
    .unionByName(age_prevalence.withColumn("segment_value", F.col("segment_value").cast("string")))
)

display(
    chronic_condition_prevalence.orderBy(F.col("condition_name"))
)

# COMMAND ----------

chronic_condition_prevalence.printSchema()

# COMMAND ----------

# %skip
chronic_condition_prevalence.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "healthcare.gold.chronic_condition_prevalence"
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from healthcare.gold.chronic_condition_prevalence