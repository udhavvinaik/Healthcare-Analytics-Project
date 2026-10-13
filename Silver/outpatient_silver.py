# Databricks notebook source
# MAGIC %md
# MAGIC # read outpatient table from bronze

# COMMAND ----------

outpatient = spark.table("healthcare.bronze.outpatientdata")

print(f"Rows: {outpatient.count()}")
print(f"Columns: {len(outpatient.columns)}")

# COMMAND ----------

display(outpatient.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC # Data type of each columns

# COMMAND ----------

outpatient.printSchema()

# COMMAND ----------

from pyspark.sql.types import DateType

if isinstance(outpatient.schema["ClaimStartDt"].dataType, DateType):
    print("Already a date column")
else:
    print("Not a date column")

# COMMAND ----------

dtype = outpatient.schema["ClaimStartDt"].dataType
print(dtype)

# COMMAND ----------

# MAGIC %md
# MAGIC # Formatting Date 

# COMMAND ----------

from pyspark.sql.functions import to_date

outpatient = (
    outpatient
    .withColumn(
        "ClaimStartDt",
        to_date("ClaimStartDt", "yyyy-MM-dd")
    )
    .withColumn(
        "ClaimEndDt",
        to_date("ClaimEndDt", "yyyy-MM-dd")
    )
)

# COMMAND ----------

from pyspark.sql.types import DateType

if isinstance(outpatient.schema["ClaimStartDt"].dataType, DateType):
    print("Already a date column")
else:
    print("Not a date column")

# COMMAND ----------

outpatient.select("ClaimStartDt", "ClaimEndDt").show(10, False)

# COMMAND ----------

# MAGIC %md
# MAGIC # checking duplicacy of ClaimID

# COMMAND ----------

from pyspark.sql.functions import col, count

duplicate_claims = (
    outpatient
    .groupBy("ClaimID")
    .agg(count("*").alias("cnt"))
    .filter(col("cnt") > 1)
)

duplicate_claims.show(truncate=False)

# COMMAND ----------

duplicate_claims.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # ClaimStartDt > ClaimEndDt handle these cases

# COMMAND ----------

invalid_dates = outpatient.filter(
    col("ClaimStartDt") > col("ClaimEndDt")
)

invalid_dates.show()

# COMMAND ----------

bad_records = outpatient.filter(
col("ClaimStartDt") > col("ClaimEndDt")
)
bad_records.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #Claim Duration Validation

# COMMAND ----------

# MAGIC %md
# MAGIC A claim spanning years is suspicious.

# COMMAND ----------

from pyspark.sql.functions import datediff, year, month

outpatient = outpatient.withColumn("ClaimDuration",datediff("ClaimEndDt", "ClaimStartDt"))\
    .withColumn("claim_year",year("ClaimStartDt"))\
    .withColumn("claim_month",month("ClaimStartDt"))
    

# COMMAND ----------

outpatient.filter(
    col("ClaimDuration") > 365
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Check Negative Reimbursement Amount

# COMMAND ----------

outpatient.select("InscClaimAmtReimbursed").show(20, False)

# COMMAND ----------

from pyspark.sql.functions import col

outpatient = outpatient.withColumn(
    "InscClaimAmtReimbursed",
    col("InscClaimAmtReimbursed").cast("int")
)

# COMMAND ----------

outpatient.schema["InscClaimAmtReimbursed"].dataType

# COMMAND ----------

outpatient.filter(
    col("InscClaimAmtReimbursed") < 0
).show()

# COMMAND ----------

outpatient.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC # check for Negative Deductible Amount

# COMMAND ----------

from pyspark.sql.functions import when, col

outpatient = outpatient.withColumn(
    "DeductibleAmtPaid",
    when(col("DeductibleAmtPaid") == "NA", None)
    .otherwise(col("DeductibleAmtPaid"))
    .cast("int")
)

# COMMAND ----------

from pyspark.sql.functions import col

outpatient = outpatient.withColumn(
    "DeductibleAmtPaid",
    col("DeductibleAmtPaid").cast("int")
)

# COMMAND ----------

outpatient.schema["DeductibleAmtPaid"].dataType

# COMMAND ----------

from pyspark.sql.functions import col

outpatient.filter(
    col("DeductibleAmtPaid") < 0
).show()

# COMMAND ----------

from pyspark.sql.functions import lit
from pyspark.sql.types import DateType

outpatient = (
    outpatient
    .withColumn("AdmissionDt", lit(None).cast(DateType()))
    .withColumn("DischargeDt", lit(None).cast(DateType()))
    .withColumn("ClaimType", lit("Outpatient"))
    .withColumn("los_days", lit(0))
)

# COMMAND ----------

outpatient.printSchema()

# COMMAND ----------

# DBTITLE 1,Rename columns to lowercase without underscores
outpatient = outpatient.toDF(*[col.lower().replace("_", "") for col in outpatient.columns])

# COMMAND ----------

from pyspark.sql import functions as F
from functools import reduce

outpatient = outpatient.withColumn(
    "diagcount",
    reduce(
        lambda x, y: x + y,
        [F.when(F.col(c).isNotNull(), 1).otherwise(0)
         for c in [f"clmdiagnosiscode{i}" for i in range(1, 11)]]
    )
).withColumn(
    "proccount",
    reduce(
        lambda x, y: x + y,
        [F.when(F.col(c).isNotNull(), 1).otherwise(0)
         for c in [f"clmprocedurecode{i}" for i in range(1, 7)]]
    )
).withColumn(
    "physiciancount",
    F.when(F.col("attendingphysician").isNotNull(), 1).otherwise(0)
    + F.when(F.col("operatingphysician").isNotNull(), 1).otherwise(0)
    + F.when(F.col("otherphysician").isNotNull(), 1).otherwise(0)
).withColumn(
    "hasoperatingphysician",
    F.when(F.col("operatingphysician").isNotNull(), 1).otherwise(0)
).withColumn(
    "totalclaimcost",
    F.coalesce(F.col("inscclaimamtreimbursed"), F.lit(0))
    + F.coalesce(F.col("deductibleamtpaid"), F.lit(0))
).withColumn(
    "dateerrorflag",
    F.when(
        (
            (F.col("claimstartdt") > F.col("claimenddt"))
            |
            (
                F.col("admissiondt").isNotNull()
                & F.col("dischargedt").isNotNull()
                & (F.col("admissiondt") > F.col("dischargedt"))
            )
        ),
        1
    ).otherwise(0)
)

# COMMAND ----------

from pyspark.sql.functions import *

outpatient_df = spark.table(
    "healthcare.bronze.outpatientdata"
)

outpatient_quarantine = (

    outpatient_df

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
            col("InscClaimAmtReimbursed") < 0,
            "Negative Reimbursement"
        )

        .when(
            col("DeductibleAmtPaid") < 0,
            "Negative Deductible"
        )

        .when(
            col("ClaimStartDt") > col("ClaimEndDt"),
            "Invalid Claim Date Range"
        )

        .when(
            col("ClaimStartDt") > current_date(),
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

display(outpatient_quarantine)

# COMMAND ----------

# MAGIC %md
# MAGIC # Save as Delta Table

# COMMAND ----------

outpatient.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("healthcare.silver.outpatientdata")

# COMMAND ----------

# MAGIC %md
# MAGIC # CDF Enable 

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC ALTER TABLE healthcare.silver.outpatientdata
# MAGIC SET TBLPROPERTIES (
# MAGIC 'delta.enableChangeDataFeed'='true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from healthcare.silver.outpatientdata

# COMMAND ----------

outpatient.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from healthcare.silver.outpatientdata