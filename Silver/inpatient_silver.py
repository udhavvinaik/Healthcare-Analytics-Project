# Databricks notebook source
inpatient_bronze = spark.table("healthcare.bronze.inpatientdata")

print(f"Rows: {inpatient_bronze.count()}")

print(f"Columns: {len(inpatient_bronze.columns)}")

display(inpatient_bronze.limit(10))

# COMMAND ----------

inpatient_bronze.printSchema()

# COMMAND ----------

from pyspark.sql.functions import count
df = spark.table("healthcare.bronze.inpatientdata")

print("Total Rows:", df.count())
display(
    df.groupBy("ClaimID")
      .agg(count("*").alias("cnt"))
      .filter("cnt > 1")
)


# COMMAND ----------

duplicate_count = df.count() - df.distinct().count()

print("Duplicate rows:", duplicate_count)

# COMMAND ----------

from pyspark.sql.functions import *

display(
    df.select([
        count(when(col(c).isNull(), c)).alias(c)
        for c in df.columns
    ])
)

# COMMAND ----------

display(
    df.select([
        count(when(col(c) == "NA", c)).alias(c)
        for c in df.columns
    ])
)

# COMMAND ----------

from pyspark.sql.functions import to_date

date_df = (
    df
    .withColumn("ClaimStartDt", to_date("ClaimStartDt"))
    .withColumn("ClaimEndDt", to_date("ClaimEndDt"))
    .withColumn("AdmissionDt", to_date("AdmissionDt"))
    .withColumn("DischargeDt", to_date("DischargeDt"))
)

display(
    date_df.filter(
        col("ClaimEndDt") < col("ClaimStartDt")
    )
)

display(
    date_df.filter(
        col("AdmissionDt") > col("DischargeDt")
    )
)

# COMMAND ----------

display(
    df.filter(
        col("InscClaimAmtReimbursed").cast("double") < 0
    )
)

# COMMAND ----------

from pyspark.sql.functions import *

silver_df = df.select([
    when(col(c) == "NA", None)
    .otherwise(col(c))
    .alias(c)
    for c in df.columns
])

# COMMAND ----------

date_cols = [
    "ClaimStartDt",
    "ClaimEndDt",
    "AdmissionDt",
    "DischargeDt"
]

for c in date_cols:
    silver_df = silver_df.withColumn(
        c,
        to_date(col(c))
    )

# COMMAND ----------

silver_df = (
    silver_df
    .withColumn(
        "InscClaimAmtReimbursed",
        col("InscClaimAmtReimbursed").cast("double")
    )
    .withColumn(
        "DeductibleAmtPaid",
        col("DeductibleAmtPaid").cast("double")
    )
)

# COMMAND ----------

silver_df = silver_df.dropDuplicates(["ClaimID"])

# COMMAND ----------

print(f"Rows: {silver_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC **NEW Columns Added**

# COMMAND ----------

silver_df = silver_df.withColumn(
    "length_of_stay_days",
    datediff(
        col("DischargeDt"),
        col("AdmissionDt")
    )
)

# COMMAND ----------

silver_df = silver_df.withColumn(
    "claim_duration_days",
    datediff(
        col("ClaimEndDt"),
        col("ClaimStartDt")
    )
)

# COMMAND ----------

#silver_df.select("TOTAL_CLAIM_COST").summary().show()

# COMMAND ----------

silver_df = silver_df.withColumn(
    "high_value_claim_flag",
    when(
        col("InscClaimAmtReimbursed") >= 13068,
        1
    ).otherwise(0)
)

# COMMAND ----------

silver_df = silver_df.withColumn(
    "claim_year",
    year("ClaimStartDt")
)

# COMMAND ----------

silver_df = silver_df.withColumn(
    "claim_month",
    month("ClaimStartDt")
)

# COMMAND ----------

silver_df = silver_df.withColumn(
    "weekend_admission_flag",
    when(
        dayofweek("AdmissionDt").isin([1,7]),
        1
    ).otherwise(0)
)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

silver_df = silver_df.withColumn(
    "_processed_at",
    current_timestamp()
)

# COMMAND ----------

print("Silver Row Count:", silver_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC # CLAIMTYPE && DIAG_COUNT && PROC_COUNT && PHYSICIAN_COUNT && HAS_OPERATING_PHYSICIAN && TOTAL_CLAIN_COST && DATE_ERROR_FLAG

# COMMAND ----------

for col_name in silver_df.columns:
    print(col_name)

# COMMAND ----------

from pyspark.sql.functions import (
    col, when,
    greatest
)
from functools import reduce

diag_cols = [f"ClmDiagnosisCode_{i}" for i in range(1, 11)]

proc_cols = [f"ClmProcedureCode_{i}" for i in range(1, 7)]

diag_count_expr = reduce(
    lambda x, y: x + y,
    [col(c).isNotNull().cast("int") for c in diag_cols]
)

proc_count_expr = reduce(
    lambda x, y: x + y,
    [col(c).isNotNull().cast("int") for c in proc_cols]
)

silver_df = (
    silver_df
    .withColumn("CLAIMTYPE", when(col("AdmissionDt").isNotNull(), "Inpatient"))

    .withColumn("DIAG_COUNT", diag_count_expr)

    .withColumn("PROC_COUNT", proc_count_expr)

    .withColumn(
        "PHYSICIAN_COUNT",
        col("AttendingPhysician").isNotNull().cast("int") +
        col("OperatingPhysician").isNotNull().cast("int") +
        col("OtherPhysician").isNotNull().cast("int")
    )

    .withColumn(
        "HAS_OPERATING_PHYSICIAN",
        when(col("OperatingPhysician").isNotNull(), 1).otherwise(0)
    )

    .withColumn(
        "TOTAL_CLAIM_COST",
        col("InscClaimAmtReimbursed") +
        col("DeductibleAmtPaid")
    )

    .withColumn(
        "DATE_ERROR_FLAG",
        when(
            (col("DischargeDt") < col("AdmissionDt")) |
            (col("ClaimEndDt") < col("ClaimStartDt")),
            1
        ).otherwise(0)
    )
)


# COMMAND ----------

silver_df.select("TOTAL_CLAIM_COST").summary().show()

# COMMAND ----------

silver_df = silver_df.withColumn(
    "high_value_claim_flag",
    when(
        col("InscClaimAmtReimbursed") >= 13068,
        1
    ).otherwise(0)
)

# COMMAND ----------

from pyspark.sql.functions import *

# Read source data

inpatient_df = spark.table(
    "healthcare.bronze.inpatientdata"
)

# Quarantine records failing business validation rules

inpatient_quarantine = (

    inpatient_df

    .withColumn(

        "quarantine_reason",

        when(
            col("ClaimID").isNull(),
            "Missing ClaimID"
        )

        .when(
            col("BeneID").isNull(),
            "Missing BeneID"
        )

        .when(
            col("Provider").isNull(),
            "Missing Provider"
        )

        .when(
            col("AttendingPhysician").isNull(),
            "Missing Attending Physician"
        )

        .when(
            expr("try_cast(InscClaimAmtReimbursed as double)") < 0,
            "Negative Reimbursement"
        )

        .when(
            expr("try_cast(DeductibleAmtPaid as double)") < 0,
            "Negative Deductible"
        )

        .when(
            expr("try_cast(AdmissionDt as date)") > expr("try_cast(DischargeDt as date)"),
            "Invalid Admission Date Range"
        )

        .when(
            expr("try_cast(ClaimStartDt as date)") > expr("try_cast(ClaimEndDt as date)"),
            "Invalid Claim Date Range"
        )

        .when(
            expr("try_cast(ClaimStartDt as date)") > current_date(),
            "Future Claim Date"
        )

        .when(
            col("ClmDiagnosisCode_1").isNull(),
            "Missing Primary Diagnosis"
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

display(inpatient_quarantine)

# COMMAND ----------

# MAGIC %md
# MAGIC #WRITE INTO THE SILVER
# MAGIC

# COMMAND ----------

silver_df = silver_df.toDF(*[col.lower().replace("_", "") for col in silver_df.columns])

# COMMAND ----------

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("healthcare.silver.inpatientdata")

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC ALTER TABLE healthcare.silver.inpatientdata
# MAGIC SET TBLPROPERTIES (
# MAGIC 'delta.enableChangeDataFeed'='true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from healthcare.silver.inpatientdata

# COMMAND ----------

silver_df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from healthcare.silver.inpatientdata

# COMMAND ----------

