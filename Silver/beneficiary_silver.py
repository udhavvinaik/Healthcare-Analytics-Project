# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import DateType
from pyspark.sql.window import Window
spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

# MAGIC %md
# MAGIC # Loading data from bronze

# COMMAND ----------

CATALOG = "healthcare"
BRONZE = f"{CATALOG}.bronze.beneficiary"
SILVER = f"{CATALOG}.silver.beneficiary"

# COMMAND ----------

bronze_df = spark.table(BRONZE)
print(bronze_df.count())
#display(bronze_df)

# COMMAND ----------

bronze_df.show()

# COMMAND ----------

bronze_df.printSchema()

# COMMAND ----------

# dropping duplicates
bronze_df = (bronze_df.dropDuplicates(["BeneID"]).withColumn("BeneID", trim(col("BeneID"))))
# DOD is NA, we are changing to none
bronze_df = bronze_df.withColumn("DOD", when(col("DOD") == "NA", None).otherwise(col("DOD")))

# COMMAND ----------

# MAGIC %md
# MAGIC # Data types Casting

# COMMAND ----------

# changing datatype from string to the desired format
silver_df = bronze_df.withColumn("DOB", to_date("DOB", "yyyy-MM-dd"))\
    .withColumn("DOD", to_date("DOD", "yyyy-MM-dd"))\
    .withColumn("Gender", col("Gender").cast("int"))\
    .withColumn("Race", col("Race").cast("int"))\
    .withColumn("State", col("State").cast("int"))\
    .withColumn("County", col("County").cast("int"))\
    .withColumn("NoOfMonths_PartACov",col("NoOfMonths_PartACov").cast("int"))\
    .withColumn("NoOfMonths_PartBCov",col("NoOfMonths_PartBCov").cast("int"))\
    .withColumn("IPAnnualReimbursementAmt",col("IPAnnualReimbursementAmt").cast("int"))\
    .withColumn("IPAnnualDeductibleAmt",col("IPAnnualDeductibleAmt").cast("int"))\
    .withColumn("OPAnnualReimbursementAmt",col("OPAnnualReimbursementAmt").cast("int"))\
    .withColumn("OPAnnualDeductibleAmt",col("OPAnnualDeductibleAmt").cast("int"))


# COMMAND ----------

# casting to int
chronic_cols = [
    "ChronicCond_Alzheimer",
    "ChronicCond_Heartfailure",
    "ChronicCond_KidneyDisease",
    "ChronicCond_Cancer",
    "ChronicCond_ObstrPulmonary",
    "ChronicCond_Depression",
    "ChronicCond_Diabetes",
    "ChronicCond_IschemicHeart",
    "ChronicCond_Osteoporasis",
    "ChronicCond_rheumatoidarthritis",
    "ChronicCond_stroke"
]

for c in chronic_cols:
    silver_df = silver_df.withColumn(c,col(c).cast("int"))

# COMMAND ----------

# MAGIC %md
# MAGIC Using 2009-12-31 as the reference date makes business sense because:
# MAGIC
# MAGIC The beneficiary data is being analyzed in conjunction with claims data.
# MAGIC Claims occur only up to the dataset's maximum claim period (around 2009).
# MAGIC We want the beneficiary's age when claims were being generated, not their age today.
# MAGIC Using current_date() would artificially inflate ages every year and make analyses non-reproducible.

# COMMAND ----------

# MAGIC %md
# MAGIC # Feature Engineering

# COMMAND ----------

# adding new columns = age, is_deceased
reference_date = to_date(lit("2009-12-31"))

silver_df = silver_df.withColumn("Age",
                                 when(col("DOD").isNotNull(),floor(months_between(col("DOD"), col("DOB")) / 12))\
                                .otherwise(floor(months_between(reference_date, col("DOB")) / 12)))\
    .withColumn("is_deceased", when(col("DOD").isNotNull(), 1).otherwise(0))

# COMMAND ----------

silver_df = (silver_df
                .withColumn("invalid_gender_flag",when(~col("Gender").isin(1,2), 1).otherwise(0))
                .withColumn("invalid_race_flag",when(~col("Race").isin(1,2,3,5), 1).otherwise(0))
                .withColumn("negative_reimbursement_flag", 
                            when((col("IPAnnualReimbursementAmt") < 0) |(col("OPAnnualReimbursementAmt") < 0),1)\
                                .otherwise(0))
            )


# COMMAND ----------

silver_df.filter(col("negative_reimbursement_flag") == 1).count()

# COMMAND ----------

display(silver_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC #AGE_GROUP && CHRONIC_COUNT && COVERAGE_GAP_MONTHS && TOTAL_ANNUAL_REIMBURSEMENT

# COMMAND ----------

for col in silver_df.columns:
    print(col)

# COMMAND ----------

from pyspark.sql.functions import col, least
from functools import reduce

chronic_cols = [
    "ChronicCond_Alzheimer",
    "ChronicCond_Heartfailure",
    "ChronicCond_KidneyDisease",
    "ChronicCond_Cancer",
    "ChronicCond_ObstrPulmonary",
    "ChronicCond_Depression",
    "ChronicCond_Diabetes",
    "ChronicCond_IschemicHeart",
    "ChronicCond_Osteoporasis",
    "ChronicCond_rheumatoidarthritis",
    "ChronicCond_stroke"
]

chronic_count_expr = reduce(
    lambda x, y: x + y,
    [(col(c) == 1).cast("int") for c in chronic_cols]
)

silver_df = (
    silver_df
    .withColumn("chronic_count", chronic_count_expr)
    .withColumn(
        "coverage_gap_month",
        12 - least(
            col("NoOfMonths_PartACov"),
            col("NoOfMonths_PartBCov")
        )
    )
    .withColumn(
        "total_annual_reimbursement",
        col("IPAnnualReimbursementAmt") +
        col("OPAnnualReimbursementAmt")
    )
)

# COMMAND ----------

silver_df.select("Age").summary().show()

# COMMAND ----------

silver_df = silver_df.withColumn(
    "age_group",
    when(col("Age") < 65, "Under 65")
    .when(col("Age") < 75, "65-74")
    .when(col("Age") < 85, "75-84")
    .otherwise("85+")
)

# COMMAND ----------

silver_df.filter(
    (col("IPAnnualReimbursementAmt") < 0) |
    (col("OPAnnualReimbursementAmt") < 0)
).select(
    "BeneID",
    "IPAnnualReimbursementAmt",
    "OPAnnualReimbursementAmt"
).show(truncate=False)

# COMMAND ----------

silver_df.count()

# COMMAND ----------

quarantine_df = (
    silver_df
    .filter(col("negative_reimbursement_flag") == 1)
    .withColumn(
        "quarantine_reason",
        lit("Negative reimbursement amount")
    )
    .withColumn(
        "quarantine_timestamp",
        current_timestamp()
    )
)

# COMMAND ----------

silver_df = silver_df.filter(
    col("negative_reimbursement_flag") == 0
)

# COMMAND ----------

silver_df.count()

# COMMAND ----------

silver_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #WRITING INTO THE SILVER

# COMMAND ----------

silver_df = silver_df.toDF(*[col.lower().replace("_", "") for col in silver_df.columns])

# COMMAND ----------

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS healthcare.quarantine;

# COMMAND ----------

(
    quarantine_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("healthcare.quarantine.beneficiary")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from healthcare.quarantine.beneficiary

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from healthcare.silver.beneficiary

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from healthcare.silver.beneficiary

# COMMAND ----------

# MAGIC %md
# MAGIC # CDF enable

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC  ALTER TABLE healthcare.silver.beneficiary
# MAGIC         SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC  ALTER TABLE healthcare.quarantine.beneficiary
# MAGIC         SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

silver_df.printSchema()

# COMMAND ----------

