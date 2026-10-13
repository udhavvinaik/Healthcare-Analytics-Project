# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- DATA ENGINEERS
# MAGIC -- Full Access
# MAGIC -- =====================================================
# MAGIC
# MAGIC GRANT USE SCHEMA ON SCHEMA healthcare.bronze TO `data_engineers`;
# MAGIC GRANT USE SCHEMA ON SCHEMA healthcare.silver TO `data_engineers`;
# MAGIC GRANT USE SCHEMA ON SCHEMA healthcare.gold TO `data_engineers`;
# MAGIC
# MAGIC GRANT SELECT ON TABLE healthcare.gold.chronic_condition_prevalence TO `data_engineers`;
# MAGIC GRANT SELECT ON TABLE healthcare.gold.fact_claims_cost TO `data_engineers`;
# MAGIC GRANT SELECT ON TABLE healthcare.gold.fraud_flag_summary TO `data_engineers`;
# MAGIC GRANT SELECT ON TABLE healthcare.gold.patient_360 TO `data_engineers`;
# MAGIC GRANT SELECT ON TABLE healthcare.gold.provider_risk_scorecard TO `data_engineers`;
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- DATA SCIENTISTS
# MAGIC -- Read Silver + Gold
# MAGIC -- =====================================================
# MAGIC
# MAGIC GRANT USE SCHEMA ON SCHEMA healthcare.silver TO `data_scientists`;
# MAGIC GRANT USE SCHEMA ON SCHEMA healthcare.gold TO `data_scientists`;
# MAGIC
# MAGIC GRANT SELECT ON TABLE healthcare.gold.chronic_condition_prevalence TO `data_scientists`;
# MAGIC GRANT SELECT ON TABLE healthcare.gold.fact_claims_cost TO `data_scientists`;
# MAGIC GRANT SELECT ON TABLE healthcare.gold.fraud_flag_summary TO `data_scientists`;
# MAGIC GRANT SELECT ON TABLE healthcare.gold.patient_360 TO `data_scientists`;
# MAGIC GRANT SELECT ON TABLE healthcare.gold.provider_risk_scorecard TO `data_scientists`;
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- DATA ANALYSTS
# MAGIC -- Gold Only
# MAGIC -- =====================================================
# MAGIC
# MAGIC GRANT USE SCHEMA ON SCHEMA healthcare.gold TO `data_analysts`;
# MAGIC
# MAGIC GRANT SELECT ON TABLE healthcare.gold.chronic_condition_prevalence TO `data_analysts`;
# MAGIC GRANT SELECT ON TABLE healthcare.gold.fact_claims_cost TO `data_analysts`;
# MAGIC GRANT SELECT ON TABLE healthcare.gold.fraud_flag_summary TO `data_analysts`;
# MAGIC GRANT SELECT ON TABLE healthcare.gold.patient_360 TO `data_analysts`;
# MAGIC GRANT SELECT ON TABLE healthcare.gold.provider_risk_scorecard TO `data_analysts`;
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- BUSINESS USERS
# MAGIC -- Dashboard Consumers
# MAGIC -- =====================================================
# MAGIC
# MAGIC GRANT USE SCHEMA ON SCHEMA healthcare.gold TO `business_users`;
# MAGIC
# MAGIC GRANT SELECT ON TABLE healthcare.gold.chronic_condition_prevalence TO `business_users`;
# MAGIC GRANT SELECT ON TABLE healthcare.gold.fact_claims_cost TO `business_users`;
# MAGIC GRANT SELECT ON TABLE healthcare.gold.fraud_flag_summary TO `business_users`;
# MAGIC GRANT SELECT ON TABLE healthcare.gold.patient_360 TO `business_users`;
# MAGIC GRANT SELECT ON TABLE healthcare.gold.provider_risk_scorecard TO `business_users`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GROUPS;