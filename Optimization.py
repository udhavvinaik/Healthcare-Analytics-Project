# Databricks notebook source
# DBTITLE 1,Gold-Layer Performance Optimization
# MAGIC %md
# MAGIC # Gold-Layer Performance Optimization
# MAGIC
# MAGIC This notebook applies eight performance optimization techniques to the healthcare Gold-layer Delta tables: partitioning, file compaction (OPTIMIZE), ZORDER clustering, broadcast joins, reusable views, aggregated summary tables, Delta auto-compaction, and column pruning for dashboard queries.

# COMMAND ----------

# ---------------------------------------------------------------------------
# Performance Optimization for Gold-layer Delta tables
# ---------------------------------------------------------------------------

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

# ---------------------------------------------------------------------------
# 1. Partition fact_claims_cost by ClaimYear
# ---------------------------------------------------------------------------
# Handles both fresh runs and recovery from partial runs (UC restricts RENAME)
_existing = spark.sql("SHOW TABLES IN healthcare.gold").collect()
_existing_names = [r.tableName for r in _existing]

if "fact_claims_cost" not in _existing_names and "fact_claims_cost_partitioned" in _existing_names:
    # Recovery: original was dropped in a partial run; recreate from partitioned copy
    spark.sql("""
      CREATE TABLE healthcare.gold.fact_claims_cost
      USING DELTA
      PARTITIONED BY (ClaimYear)
      AS SELECT * FROM healthcare.gold.fact_claims_cost_partitioned
    """)
    spark.sql("DROP TABLE IF EXISTS healthcare.gold.fact_claims_cost_partitioned")
elif "fact_claims_cost" in _existing_names:
    # Normal path: create partitioned copy, swap, clean up
    spark.sql("""
      CREATE OR REPLACE TABLE healthcare.gold.fact_claims_cost_partitioned
      USING DELTA
      PARTITIONED BY (ClaimYear)
      AS SELECT * FROM healthcare.gold.fact_claims_cost
    """)
    spark.sql("DROP TABLE IF EXISTS healthcare.gold.fact_claims_cost")
    spark.sql("""
      CREATE TABLE healthcare.gold.fact_claims_cost
      USING DELTA
      PARTITIONED BY (ClaimYear)
      AS SELECT * FROM healthcare.gold.fact_claims_cost_partitioned
    """)
    spark.sql("DROP TABLE IF EXISTS healthcare.gold.fact_claims_cost_partitioned")
print("✅ Partitioned fact_claims_cost by ClaimYear")

# ---------------------------------------------------------------------------
# 2. OPTIMIZE Gold Delta tables (compacts small files)
# ---------------------------------------------------------------------------
gold_tables = [
    "healthcare.gold.fact_claims_cost",
    "healthcare.gold.patient_360",
    "healthcare.gold.provider_risk_scorecard",
    "healthcare.gold.chronic_condition_prevalence",
    "healthcare.gold.fraud_flag_summary",
]

for table in gold_tables:
    spark.sql(f"OPTIMIZE {table}")
    print(f"✅ OPTIMIZE completed on {table}")

# ---------------------------------------------------------------------------
# 3. ZORDER BY on key dimension IDs for faster point lookups
# ---------------------------------------------------------------------------
spark.sql("OPTIMIZE healthcare.gold.fact_claims_cost ZORDER BY (BeneficiaryID, ProviderID)")
print("✅ ZORDER BY applied on BeneficiaryID and ProviderID")

# ---------------------------------------------------------------------------
# 4. Broadcast joins for dimension lookups (small dims → broadcast)
# ---------------------------------------------------------------------------
dim_patient   = spark.table("healthcare.gold.patient_360")
dim_provider  = spark.table("healthcare.gold.provider_risk_scorecard")

fact_claims = (
    spark.table("healthcare.gold.fact_claims_cost")
        .join(broadcast(dim_patient),   F.col("BeneficiaryID") == F.col("beneid"),   "left")
        .join(broadcast(dim_provider),  F.col("ProviderID")    == F.col("provider"),  "left")
)
print("✅ Broadcast joins enabled for dimension lookups")

# ---------------------------------------------------------------------------
# 5. Cache reusable DataFrames during Gold transformations
# ---------------------------------------------------------------------------
# On serverless Spark Connect, .cache() triggers PERSIST TABLE which is
# unsupported. Use a temp view for reusability instead — serverless
# auto-caches hot data through Adaptive Query Execution.
fact_claims.createOrReplaceTempView("fact_claims_enriched")
spark.table("fact_claims_enriched").count()  # materialize via AQE
print("✅ Reusable fact_claims view materialized (serverless auto-cache)")

# ---------------------------------------------------------------------------
# 6. Create aggregated summary tables for dashboards
# ---------------------------------------------------------------------------
spark.sql("""
  CREATE OR REPLACE TABLE healthcare.gold.agg_claims_cost_summary
  USING DELTA
  AS
  SELECT
      ClaimYear,
      ProviderID,
      BeneficiaryID,
      COUNT(*)               AS ClaimCount,
      SUM(TotalClaimCost)    AS TotalClaimCost,
      AVG(TotalClaimCost)    AS AvgClaimCost,
      MAX(TotalClaimCost)    AS MaxClaimCost,
      MIN(TotalClaimCost)    AS MinClaimCost
  FROM healthcare.gold.fact_claims_cost
  GROUP BY ClaimYear, ProviderID, BeneficiaryID
""")
print("✅ Created aggregated summary table: healthcare.gold.agg_claims_cost_summary")

# ---------------------------------------------------------------------------
# 7. Enable Delta Lake optimization features
# ---------------------------------------------------------------------------
spark.sql("""
  ALTER TABLE healthcare.gold.fact_claims_cost SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'      = 'true',
    'delta.autoOptimize.autoCompact'       = 'true',
    'delta.deletedFileRetentionDuration'   = 'interval 7 days',
    'delta.logRetentionDuration'            = 'interval 30 days'
  )
""")

spark.sql("""
  ALTER TABLE healthcare.gold.agg_claims_cost_summary SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
  )
""")
print("✅ Enabled Delta Lake optimization features (auto-compaction, optimize writes)")

# ---------------------------------------------------------------------------
# 8. Minimize data scans — select only required columns
# ---------------------------------------------------------------------------
dashboard_df = (
    spark.table("healthcare.gold.agg_claims_cost_summary")
         .select("ClaimYear", "ProviderID", "TotalClaimCost", "ClaimCount")
         .filter(F.col("ClaimYear") >= 2008)
)
#display(dashboard_df)
print("✅ Minimized data scans by selecting only required columns")

# Release the temp view when done
spark.catalog.dropTempView("fact_claims_enriched")
print("\n🎉 All performance optimizations applied successfully.")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from healthcare.gold.patient_360

# COMMAND ----------

