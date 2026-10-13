# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

claims_enriched_df = spark.table(
    "healthcare.silver.claim_enriched"
)

# COMMAND ----------

claims_enriched_df.printSchema()

# COMMAND ----------

# Create Fraud Summary Metrics
fraud_summary = (
    claims_enriched_df
    .groupBy(
        "potentialfraud"
    )
    .agg(
        # Number of providers
        countDistinct(
            "provider"
        ).alias(
            "provider_count"
        ),
        # Number of claims
        count(
            "*"
        ).alias(
            "claim_count"
        ),
        # Unique patients
        countDistinct(
            "beneid"
        ).alias(
            "patient_count"
        ),
        # Total claim amount
        round(
            sum(
                "inscclaimamtreimbursed"
            ),
            2
        ).alias(
            "total_claim_amount"
        ),
        # Average claim amount
        round(
            avg(
                "inscclaimamtreimbursed"
            ),
            2
        ).alias(
            "avg_claim_amount"
        ),
        # High-value claims
        sum(
            when(
                col("highvalueclaimflag") == 1,
                1
            ).otherwise(0)
        ).alias(
            "high_value_claim_count"
        )
    )
)

# COMMAND ----------

total_claims = claims_enriched_df.count()

total_amount = claims_enriched_df.agg(
    sum("inscclaimamtreimbursed")
).collect()[0][0]

# COMMAND ----------

fraud_summary = (
    fraud_summary
    .withColumn(
        "claim_percentage",
        round(
            (
                col("claim_count")
                /
                lit(total_claims)
            ) * 100,
            2
        )
    )
    .withColumn(
        "claim_amount_percentage",
        round(
            (
                col("total_claim_amount")
                /
                lit(total_amount)
            ) * 100,
            2
        )
    )
)

# COMMAND ----------

fraud_summary = (
    fraud_summary
    .withColumn(
        "gold_load_ts",
        current_timestamp()
    )
)

# COMMAND ----------

display(fraud_summary)

# COMMAND ----------

(
    fraud_summary.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "healthcare.gold.fraud_flag_summary"
    )
)