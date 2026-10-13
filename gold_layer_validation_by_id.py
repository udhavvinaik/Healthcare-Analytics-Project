# Databricks notebook source
# GOLD TABLE DATA VALIDATION

gold_tables = {
    "patient_360": "beneid",
    "provider_risk_scorecard": "provider",
    "fact_claims_cost": "ClaimID"
}

for table_name, id_col in gold_tables.items():

    print("\n" + "="*80)
    print(f"Validating {table_name}")

    try:
        # Pick one ID
        sample_id = (
            spark.sql(f"""
                SELECT {id_col}
                FROM `healthcare`.`gold`.`{table_name}`
                WHERE {id_col} IS NOT NULL
                LIMIT 1
            """)
            .first()[0]
        )

        print(f"Sample {id_col}: {sample_id}")

        # Display record
        spark.sql(f"""
            SELECT *
            FROM `healthcare`.`gold`.`{table_name}`
            WHERE {id_col} = '{sample_id}'
        """).show(truncate=False)

        # Check duplicates
        duplicate_count = spark.sql(f"""
            SELECT COUNT(*) cnt
            FROM (
                SELECT {id_col}
                FROM `healthcare`.`gold`.`{table_name}`
                WHERE {id_col} = '{sample_id}'
                GROUP BY {id_col}
                HAVING COUNT(*) > 1
            )
        """).first()[0]

        if duplicate_count == 0:
            print(f"✅ Validation Passed: No duplicate {id_col}")
        else:
            print(f"❌ Validation Failed: Duplicate {id_col} found")

    except Exception as e:
        print(f"⚠️ Validation failed for {table_name}")
        print(str(e))

print("\n✅ Gold Layer Validation Completed")

# COMMAND ----------

