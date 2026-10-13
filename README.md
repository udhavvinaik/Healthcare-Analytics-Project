# Healthcare Analytics Lakehouse Project

## Overview

This project implements an end-to-end Healthcare Analytics Lakehouse solution using the Medallion Architecture (Bronze, Silver, and Gold layers). The solution ingests healthcare claims and patient data, performs data quality validations and transformations, applies governance controls, and delivers actionable business insights through interactive dashboards.

---

## Architecture

```text
Raw Data
   │
   ▼
Bronze Layer
(Raw Ingestion)
   │
   ▼
Silver Layer
(Cleansing & Validation)
   │
   ▼
Gold Layer
(Business Aggregations)
   │
   ▼
Dashboards & Analytics
```

---

## Tech Stack

- Databricks
- Apache Spark
- Delta Lake
- SQL
- Unity Catalog
- Databricks Dashboards
- RBAC (Role Based Access Control)
- ABAC (Attribute Based Access Control)
- GitHub

---

## Project Features

### Bronze Layer

- Raw healthcare data ingestion
- Schema enforcement
- Historical data retention
- Source system integration

### Silver Layer

- Data cleansing
- Standardization
- Null handling
- Deduplication
- Business rule implementation
- Data quality validation

### Gold Layer

- Curated business datasets
- KPI calculations
- Analytical aggregations
- Reporting-ready tables

---

## Gold Tables

### Patient 360

A comprehensive patient-centric view that combines demographics, healthcare utilization, chronic conditions, and claims information.

### Provider Risk Scorecard

Provider performance analytics including risk indicators, reimbursement trends, and utilization metrics.

### Claims Cost Summary

Aggregated claims analysis providing visibility into healthcare spending, claim frequency, and reimbursement trends.

### Chronic Condition Prevalence

Analysis of chronic disease distribution and prevalence across the patient population.

---

## Data Quality Validation

Implemented validation checks including:

- Record count reconciliation
- Null value checks
- Duplicate detection
- Referential integrity validation
- Outlier detection using Z-Score analysis
- Cross-table consistency checks

---

## Governance & Security

### RBAC

Roles configured for:

- Admin
- Data Engineer
- Analyst
- Business User

### ABAC

Implemented attribute-based controls including:

- State-level row filtering
- User-based access segregation
- Data access governance policies

---

## Performance Optimization

Implemented Delta Lake optimization strategies:

```sql
OPTIMIZE table_name;
VACUUM table_name;
```

---

## Dashboards

### Executive Dashboard

- Total Patients
- Total Claims
- Total Reimbursements
- High-Risk Patient Count

### Provider Analytics Dashboard

- Provider Risk Analysis
- Provider Performance Comparison
- Cost Trends

### Claims Analytics Dashboard

- Claims Distribution
- Monthly Cost Trends
- Claims Volume Analysis

### Chronic Disease Dashboard

- Disease Prevalence
- Geographic Distribution
- Patient Risk Segmentation

---

## Key Business Outcomes

- Unified Patient 360 view
- Provider performance monitoring
- Healthcare cost transparency
- High-risk patient identification
- Secure and governed data access
- Scalable Lakehouse architecture

---

## Repository Structure

```text
healthcare-analytics/
│
├── Bronze/
├── Silver/
├── Gold/
├── Other Files for Governance and Dashboards
│
└── README.md
```
