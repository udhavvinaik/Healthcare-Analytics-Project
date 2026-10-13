# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS healthcare;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE CATALOG
# MAGIC ON CATALOG healthcare
# MAGIC TO `healthcare_team`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON CATALOG healthcare;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS healthcare.bronze;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS healthcare.silver;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS healthcare.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY
# MAGIC ON SCHEMA healthcare.bronze
# MAGIC TO `healthcare_team`;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY
# MAGIC ON SCHEMA healthcare.silver
# MAGIC TO `healthcare_team`;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY
# MAGIC ON SCHEMA healthcare.gold
# MAGIC TO `healthcare_team`;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME healthcare.bronze.raw_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT ALL PRIVILEGES
# MAGIC ON CATALOG healthcare
# MAGIC TO `healthcare_team`;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT ALL PRIVILEGES
# MAGIC ON SCHEMA healthcare.bronze
# MAGIC TO `healthcare_team`;
# MAGIC
# MAGIC GRANT ALL PRIVILEGES
# MAGIC ON SCHEMA healthcare.silver
# MAGIC TO `healthcare_team`;
# MAGIC
# MAGIC GRANT ALL PRIVILEGES
# MAGIC ON SCHEMA healthcare.gold
# MAGIC TO `healthcare_team`;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT READ VOLUME,
# MAGIC       WRITE VOLUME
# MAGIC ON VOLUME healthcare.bronze.raw_data
# MAGIC TO `healthcare_team`;

# COMMAND ----------

