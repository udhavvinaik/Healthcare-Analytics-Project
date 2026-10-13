# Databricks notebook source
claims_enriched = spark.table("healthcare.silver.claim_enriched")


# COMMAND ----------

claims_enriched.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *

provider_scorecard = (
    claims_enriched
    .groupBy("provider")
    .agg(
        count("claimid").alias("total_claims"),
        countDistinct("beneid").alias("unique_patients"),
        sum("inscclaimamtreimbursed").alias("total_reimbursed"),
        avg("losdays").alias("avg_los_days"),
        countDistinct("attendingphysician").alias("unique_attending_physicians"),
        avg("chroniccount").alias("avg_chronic_count"),

        # Inpatient share
        (
            sum(when(col("claimtype") == "Inpatient", 1).otherwise(0))
            / count("claimid")
        ).alias("ip_claim_share"),

        # High cost claim share
        (
            sum(when(col("highvalueclaimflag") == 1, 1).otherwise(0))
            / count("claimid")
        ).alias("pct_high_cost_claims"),

        first("potentialfraud", ignorenulls=True).alias("potentialfraud")
    )
    .withColumn(
        "avg_claim_amt",
        col("total_reimbursed") / col("total_claims")
    )
    .withColumn(
        "claims_per_patient",
        col("total_claims") / col("unique_patients")
    )
)

# COMMAND ----------

display(provider_scorecard.limit(5))

# COMMAND ----------

# claims.groupBy("provider").agg(
#     F.avg("inscclaimamtreimbursed")
# )

# COMMAND ----------


from pyspark.sql import functions as F

# COMMAND ----------

from pyspark.sql.functions import col, lit
from functools import reduce

metrics = [
    "total_claims",
    "avg_claim_amt",
    "ip_claim_share",
    "avg_los_days",
    "claims_per_patient",
    "pct_high_cost_claims",
    "avg_chronic_count"
]

risk_df = provider_scorecard

for m in metrics:
    stats = risk_df.selectExpr(
        f"avg({m}) as mean",
        f"stddev({m}) as std"
    ).collect()[0]

    mean_val = stats["mean"]
    std_val = stats["std"] if stats["std"] else 1

    risk_df = risk_df.withColumn(
        f"{m}_z",
        (col(m) - lit(mean_val)) / lit(std_val)
    )

risk_df = risk_df.withColumn(
    "risk_score",
    reduce(
        lambda x, y: x + y,
        [col(f"{m}_z") for m in metrics]
    )
)

# COMMAND ----------

risk_df = risk_df.withColumn(
    "risk_band",
    F.when(F.col("risk_score") >= 80, "Very High")
     .when(F.col("risk_score") >= 60, "High")
     .when(F.col("risk_score") >= 40, "Medium")
     .otherwise("Low")
)

# COMMAND ----------

risk_df = (
    risk_df
    .withColumn(
        "risk_tier",
        when(col("risk_score") >= 5, "High")
        .when(col("risk_score") >= 2, "Medium")
        .otherwise("Low")
    )
)

# COMMAND ----------

display(risk_df)

# COMMAND ----------

provider_risk_scorecard = risk_df.select(
    "provider",
    "total_claims",
    "unique_patients",
    "total_reimbursed",
    "avg_claim_amt",
    "ip_claim_share",
    "avg_los_days",
    "unique_attending_physicians",
    "claims_per_patient",
    "pct_high_cost_claims",
    "avg_chronic_count",
    "risk_score",
    "risk_tier",
    "potentialfraud"
)

display(provider_risk_scorecard)

# COMMAND ----------

# MAGIC %skip
# MAGIC provider_risk_scorecard.write \
# MAGIC     .format("delta") \
# MAGIC     .mode("overwrite") \
# MAGIC     .saveAsTable("healthcare.gold.provider_risk_scorecard")

# COMMAND ----------

import matplotlib.pyplot as plt

df = spark.table("healthcare.gold.provider_risk_scorecard").toPandas()

fig, axes = plt.subplots(2, 2, figsize=(16, 12))

# Top 10 Risk Providers
top_risk = df.sort_values("risk_score", ascending=False).head(10)
axes[0,0].bar(top_risk["provider"], top_risk["risk_score"])
axes[0,0].set_title("Top 10 Risk Providers")
axes[0,0].tick_params(axis='x', rotation=90)

# Risk Tier Distribution
risk_dist = df["risk_tier"].value_counts()
axes[0,1].pie(
    risk_dist.values,
    labels=risk_dist.index,
    autopct='%1.1f%%'
)
axes[0,1].set_title("Risk Tier Distribution")

# Top Reimbursement Providers
top_reimb = df.sort_values("total_reimbursed", ascending=False).head(10)
axes[1,0].bar(top_reimb["provider"], top_reimb["total_reimbursed"])
axes[1,0].set_title("Top Reimbursement Providers")
axes[1,0].tick_params(axis='x', rotation=90)

# Risk Score vs Reimbursement
axes[1,1].scatter(
    df["total_reimbursed"],
    df["risk_score"]
)
axes[1,1].set_title("Risk Score vs Reimbursement")
axes[1,1].set_xlabel("Total Reimbursed")
axes[1,1].set_ylabel("Risk Score")

plt.tight_layout()
plt.show()

# COMMAND ----------

