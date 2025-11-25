#  📦 E-Commerce Data Warehouse (End-to-End Modern Data Platform)

 

A cloud-native Data Lakehouse pipeline built using AWS S3, Snowflake, dbt, and Dagster, following the Medallion Architecture (Bronze → Silver → Gold) with enterprise grade Data Governance features including RBAC, Dynamic Data Masking, and Row-Level Security (RLS).

This project demonstrates real-world data engineering and Data Governance best practices used in modern analytics platforms.

## ⭐ Architecture Overview
<p align="center"> <img src="YOUR_ARCHITECTURE_IMAGE.png" width="700"/> </p>

### Tech Stack

* Python — Ingestion and automation scripts

* AWS S3 — Data Lake & raw data landing zone

* Snowflake — Cloud Data Warehouse 
    * Stages, External Tables, Secure Views

* dbt — SQL transformations, lineage, testing, and SCD Type 2

* Dagster — Orchestration, scheduling, and observability

## Data Platform Layers 
#### 1. Data Lake (AWS S3 — Bronze Layer)

Raw data from source systems lands in Amazon S3, acting as the Data Lake.

* Stores raw files

* Some datasets are partitioned in S3 (e.g., partitioned by ingestion_date) to improve query performance and reduce scan costs.

* An ingestion_state.json file is maintained in S3 to track the last successfully ingested timestamp, enabling reliable incremental ingestion.

* Supports schema-on-read

* Connected to Snowflake via External Tables

* High durability, low cost 

#### 2. Snowflake Warehouse (Bronze → Silver → Gold)

**Bronze Layer**

* Snowflake External Tables query raw files directly from S3.

* Minimal transformations applied.

* Raw, audit-ready datasets.

**Silver Layer**

Cleansed and modeled data created using **dbt** :

* Staging tables

* Dimensional modeling (facts & dims)

* Fact tables

* SCD Type 2 dimension tables (historical tracking)


**Gold Layer (Data Marts)** 

* Aggregated business-ready tables

* Optimized for BI & analytics

* Supports dashboards, RFM analysis, product analytics, segmentation, and KPIs

### Dimensional Modeling
**Fact Tables**

* fact_orders

* fact_payments

* fact_customers

Dimension Tables

* dim_customers (SCD Type 2)

* dim_products (SCD Type 2)

* dim_resellers (SCD Type 2)

#### Slowly Changing Dimensions (Type 2)

* Preserves full historical context of customer/product attributes

* Enables accurate point-in-time reporting and temporal analysis

### dbt Transformations
<p align="center"> <img src="images/data_lineage.png" width="700"/> </p>
All transformation and modeling logic is implemented in dbt, making the entire warehouse framework fully modular, testable, and reproducible.

* dbt handles all transformations across Bronze → Silver → Gold

* Handles staging, cleaning, SCD2 logic, and full dimensional modeling

* SQL models stored in Git for version control

* Data quality tests 

* Snapshots maintain SCD Type 2 history

* Lineage graph provides complete end-to-end traceability


### 🔐 Enterprise Data Governance (Snowflake)

All governance controls are implemented natively in Snowflake, leveraging its built-in security model.

#### Role-Based Access Control (RBAC)

Defined roles:

* **DATA_ENGINEER** — full access to raw & transformed layers

* **DATA_SCIENTIST** — analytical access to Silver/Gold

* **DATA_ANALYST** — restricted, masked, or view-only access

#### Dynamic Data Masking

* Automatic masking applied to sensitive PII(emails and Phone numbers)

* Analysts see masked outputs

* Engineers see full data (based on role hierarchy)

#### Row-Level Security (RLS)

* Row access based on Country

* Perfect for multi-region analytics and regulatory compliance.

### Orchestration with Dagster
<p align="center"> <img src="YOUR_DAGSTER_PIPELINE.png" width="700"/> </p>

Dagster orchestrates the full workflow:

* Ingest raw data → S3

* Refresh Snowflake external tables

* dbt transformations (Bronze → Silver → Gold)

* Run data tests & documentation


Features:

* Scheduling

* Dependency management

* Logs & monitoring