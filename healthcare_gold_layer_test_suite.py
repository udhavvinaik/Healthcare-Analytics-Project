# Databricks notebook source
from pyspark.sql.functions import *

print("=" * 80)
print("HEALTHCARE GOLD LAYER TEST SUITE")
print("=" * 80)

results = []

def run_test(test_name, condition):
    try:
        assert condition
        results.append((test_name, "PASS"))
        print(f"PASS : {test_name}")
    except:
        results.append((test_name, "FAIL"))
        print(f"FAIL : {test_name}")

# ==========================================================
# PROVIDER RISK SCORECARD
# ==========================================================

print("\nTesting Provider Risk Scorecard...")

provider_df = spark.table(
    "healthcare.gold.provider_risk_scorecard"
)

run_test(
    "Provider Risk Scorecard Not Empty",
    provider_df.count() > 0
)

run_test(
    "Provider Unique",
    provider_df.groupBy("provider")
    .count()
    .filter(col("count") > 1)
    .count() == 0
)

run_test(
    "Provider Not Null",
    provider_df.filter(
        col("provider").isNull()
    ).count() == 0
)

run_test(
    "Risk Score Exists",
    "risk_score" in provider_df.columns
)

run_test(
    "Risk Tier Valid",
    provider_df.filter(
        ~col("risk_tier").isin(
            "High",
            "Medium",
            "Low"
        )
    ).count() == 0
)

run_test(
    "Total Reimbursed Non Negative",
    provider_df.filter(
        col("total_reimbursed") < 0
    ).count() == 0
)

run_test(
    "Total Claims Positive",
    provider_df.filter(
        col("total_claims") <= 0
    ).count() == 0
)

run_test(
    "Claims Per Patient Positive",
    provider_df.filter(
        col("claims_per_patient") <= 0
    ).count() == 0
)

run_test(
    "Average Claim Amount Non Negative",
    provider_df.filter(
        col("avg_claim_amt") < 0
    ).count() == 0
)

# ==========================================================
# PATIENT 360
# ==========================================================

print("\nTesting Patient 360...")

patient_df = spark.table(
    "healthcare.gold.patient_360"
)

run_test(
    "Patient 360 Not Empty",
    patient_df.count() > 0
)

run_test(
    "BeneID Unique",
    patient_df.groupBy("beneid")
    .count()
    .filter(col("count") > 1)
    .count() == 0
)

run_test(
    "Age Non Negative",
    patient_df.filter(
        col("age") < 0
    ).count() == 0
)

run_test(
    "Chronic Count Non Negative",
    patient_df.filter(
        col("chronic_count") < 0
    ).count() == 0
)

run_test(
    "Total Claims Positive",
    patient_df.filter(
        col("total_claims") < 0
    ).count() == 0
)

run_test(
    "IP Claims Positive",
    patient_df.filter(
        col("ip_claims") < 0
    ).count() == 0
)

run_test(
    "OP Claims Positive",
    patient_df.filter(
        col("op_claims") < 0
    ).count() == 0
)

run_test(
    "Total Paid Non Negative",
    patient_df.filter(
        col("total_paid") < 0
    ).count() == 0
)

run_test(
    "Distinct Providers Positive",
    patient_df.filter(
        col("distinct_providers") < 0
    ).count() == 0
)

run_test(
    "Coverage Gap Months Non Negative",
    patient_df.filter(
        col("coverage_gap_months") < 0
    ).count() == 0
)

# ==========================================================
# CLAIMS COST FACT
# ==========================================================

print("\nTesting Claims Cost Fact...")

claims_df = spark.table(
    "healthcare.gold.fact_claims_cost"
)

run_test(
    "Claims Cost Fact Not Empty",
    claims_df.count() > 0
)

run_test(
    "ClaimID Unique",
    claims_df.groupBy("claimid")
    .count()
    .filter(col("count") > 1)
    .count() == 0
)

if "totalclaimcost" in claims_df.columns:
    run_test(
        "Total Claim Cost Non Negative",
        claims_df.filter(
            col("totalclaimcost") < 0
        ).count() == 0
    )

if "losdays" in claims_df.columns:
    run_test(
        "LOS Days Non Negative",
        claims_df.filter(
            col("losdays") < 0
        ).count() == 0
    )

if "claimduration" in claims_df.columns:
    run_test(
        "Claim Duration Non Negative",
        claims_df.filter(
            col("claimduration") < 0
        ).count() == 0
    )

# ==========================================================
# CHRONIC CONDITION PREVALENCE
# ==========================================================

print("\nTesting Chronic Condition Prevalence...")

chronic_df = spark.table(
    "healthcare.gold.chronic_condition_prevalence"
)

run_test(
    "Chronic Condition Table Not Empty",
    chronic_df.count() > 0
)

if "prevalence_pct" in chronic_df.columns:

    run_test(
        "Prevalence Between 0 and 100",
        chronic_df.filter(
            (col("prevalence_pct") < 0) |
            (col("prevalence_pct") > 100)
        ).count() == 0
    )

# ==========================================================
# FRAUD FLAG SUMMARY
# ==========================================================

print("\nTesting Fraud Flag Summary...")

fraud_df = spark.table(
    "healthcare.gold.fraud_flag_summary"
)

run_test(
    "Fraud Summary Not Empty",
    fraud_df.count() > 0
)

if (
    "fraud_providers" in fraud_df.columns and
    "total_providers" in fraud_df.columns
):

    run_test(
        "Fraud Providers <= Total Providers",
        fraud_df.filter(
            col("fraud_providers") >
            col("total_providers")
        ).count() == 0
    )

# ==========================================================
# SUMMARY
# ==========================================================

print("\n" + "=" * 80)
print("TEST EXECUTION SUMMARY")
print("=" * 80)

summary_df = spark.createDataFrame(
    results,
    ["Test Case", "Status"]
)

display(summary_df)

total_tests = len(results)
passed_tests = len(
    [r for r in results if r[1] == "PASS"]
)
failed_tests = len(
    [r for r in results if r[1] == "FAIL"]
)

print(f"Total Tests  : {total_tests}")
print(f"Passed Tests : {passed_tests}")
print(f"Failed Tests : {failed_tests}")

if failed_tests == 0:
    print("\n✅ ALL GOLD LAYER TESTS PASSED")
else:
    print("\n❌ SOME TESTS FAILED - REVIEW REQUIRED")

print("=" * 80)

# COMMAND ----------

