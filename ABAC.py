# Databricks notebook source
# MAGIC %md
# MAGIC ABAC Controls Implemented
# MAGIC
# MAGIC 1. Row-Level Security
# MAGIC    - State-based access control
# MAGIC    - Users see only authorized states
# MAGIC
# MAGIC 2. Column Masking
# MAGIC    - DOB masked for non-authorized users
# MAGIC    - Race masked for non-authorized users
# MAGIC    - Gender masked for non-authorized users
# MAGIC
# MAGIC 3. Compliance Access
# MAGIC    - Compliance Officers see full PII
# MAGIC
# MAGIC 4. Analyst Access
# MAGIC    - Analysts consume Age and Age_Group instead of direct PII
# MAGIC
# MAGIC Business Benefit:
# MAGIC Healthcare-sensitive information remains protected while
# MAGIC still enabling analytics and reporting.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS healthcare.security;

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE healthcare.security.user_state_access
# MAGIC (
# MAGIC     user_email STRING,
# MAGIC     state INT
# MAGIC );

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC INSERT INTO healthcare.security.user_state_access
# MAGIC VALUES
# MAGIC ('analyst_ca@company.com', 5),
# MAGIC ('analyst_tx@company.com', 45),
# MAGIC ('analyst_ny@company.com', 39);

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION healthcare.security.state_row_filter
# MAGIC (state INT)
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN EXISTS
# MAGIC (
# MAGIC     SELECT 1
# MAGIC     FROM healthcare.security.user_state_access u
# MAGIC     WHERE lower(u.user_email) = lower(current_user())
# MAGIC       AND u.state = state
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from healthcare.gold.patient_360

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC ALTER TABLE healthcare.gold.patient_360
# MAGIC SET ROW FILTER healthcare.security.state_row_filter
# MAGIC ON (State);

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE healthcare.gold.patient_360
# MAGIC DROP ROW FILTER;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION healthcare.security.mask_dob
# MAGIC (d DATE)
# MAGIC RETURNS DATE
# MAGIC RETURN
# MAGIC
# MAGIC CASE
# MAGIC
# MAGIC     WHEN is_account_group_member('admins')
# MAGIC     THEN d
# MAGIC
# MAGIC     WHEN is_account_group_member('compliance_officers')
# MAGIC     THEN d
# MAGIC
# MAGIC     ELSE NULL
# MAGIC
# MAGIC END;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE healthcare.silver.beneficiary
# MAGIC
# MAGIC ALTER COLUMN dob
# MAGIC
# MAGIC SET MASK healthcare.security.mask_dob;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION healthcare.security.mask_race
# MAGIC (r INT)
# MAGIC RETURNS INT
# MAGIC RETURN
# MAGIC
# MAGIC CASE
# MAGIC
# MAGIC     WHEN is_account_group_member('admins')
# MAGIC     THEN r
# MAGIC
# MAGIC     WHEN is_account_group_member('compliance_officers')
# MAGIC     THEN r
# MAGIC
# MAGIC     ELSE -1
# MAGIC
# MAGIC END;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE healthcare.silver.beneficiary
# MAGIC
# MAGIC ALTER COLUMN race
# MAGIC
# MAGIC SET MASK healthcare.security.mask_race;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION healthcare.security.mask_gender
# MAGIC (g INT)
# MAGIC RETURNS INT
# MAGIC RETURN
# MAGIC
# MAGIC CASE
# MAGIC
# MAGIC     WHEN is_account_group_member('admins')
# MAGIC     THEN g
# MAGIC
# MAGIC     WHEN is_account_group_member('compliance_officers')
# MAGIC     THEN g
# MAGIC
# MAGIC     ELSE NULL
# MAGIC
# MAGIC END;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE healthcare.silver.beneficiary
# MAGIC
# MAGIC ALTER COLUMN gender
# MAGIC
# MAGIC SET MASK healthcare.security.mask_gender;
# MAGIC

# COMMAND ----------

