# Netflix Data Pipeline on Azure

Overview

# This project develops a batch-oriented data pipeline to process Netflix-related data using Azure Data Factory (ADF), Databricks, and Delta Live Tables (DLT). The objective is to build a scalable and automated Weekly sheduled ETL process that enables periodic data ingestion, transformation, and analysis.

# Architecture

![architecture](https://github.com/user-attachments/assets/fec80916-0407-4a84-99e9-bbbe3fa8bb8e)


The pipeline follows the Medallion Architecture:

Bronze Layer: Raw data is periodically ingested from GitHub into Azure Data Lake Storage (ADLS) via ADF using parameterized pipline with validations

Silver Layer: Data cleansing and transformation occur using Databricks Auto-loader and PySpark where the pipeline is scheduled on every Mondays

Gold Layer: Refined data is stored efficiently using Delta Live Tables (DLT) for optimized querying.

Technology Stack

Azure Data Factory (ADF) – Manages batch data movement.

Azure Databricks & PySpark – Performs data transformation.

Access Connector:To connect unity catolog with ADLS gen2 (creating metastore on ADLS gen2)

Databricks Auto-loader – Handles scheduled incremental data ingestion.

Delta Live Tables (DLT) – Supports optimized storage with schema evolution.

Azure Data Lake Storage (ADLS) – Stores raw, processed, and curated data.

# Implementation Steps

# Step 1: Setting Up Azure Infrastructure
![Resource Group](https://github.com/user-attachments/assets/a60d39f0-5bdb-454c-9e05-feee53ba8239)

Create an Azure Resource Group.

Deploy Azure Data Lake Storage Gen2.

Set up Azure Data Factory (ADF).

Configure an Azure Databricks workspace and clusters.

# Step 2: Data Ingestion Using ADF

![ADF (1)](https://github.com/user-attachments/assets/afbadeee-3b40-4a2e-8a73-314d131b5e3e)


Configure Linked Services for GitHub (HTTP Connector) and ADLS.

Develop parameterized ADF pipelines to enable dynamic batch ingestion.

Implement file existence validation to ensure data integrity.

Use Copy Activity to periodically move raw data from GitHub to ADLS (Bronze Layer).

# Step 3: Access Connector

Add Access Connector to resourece group such that Azure Data Lake Storage Gen2 is used to enable Unity Catalog in Azure Databricks to securely manage and access data stored in ADLS Gen2 without requiring direct storage credentials.

# Step 4: Data Processing in Databricks
![workspace_adb](https://github.com/user-attachments/assets/72e4b354-ddcb-46f9-b27b-b9fdf594ec1a)

![workflow_load_dim_silver](https://github.com/user-attachments/assets/d1b0ea4b-b535-4144-b4be-c7fa6d8f7167)

![workflow_load_fact_silver](https://github.com/user-attachments/assets/e50a984c-9163-426e-8a85-c70490168fa2)



Develop a Databricks Notebook for batch data transformation.

Utilize Auto-loader for scheduled incremental ingestion, ensuring fact table pipelines run on Mondays.

Apply PySpark transformations for data cleaning and structuring.

Store processed data in the Silver Layer for further refinement.

# Step 5: Implementing Delta Live Tables (DLT)

Enable Delta Live Tables in Databricks.

Design ETL workflows to transform Silver Layer data into the Gold Layer.

Process batch-based dimensional data tables for analytical queries.

Optimize storage using schema evolution to handle data structure changes.

# Conclusion

This project builds a batch-based data pipeline for Netflix data analysis. By utilizing Azure Data Factory, Databricks, and Delta Live Tables, the pipeline ensures efficient and automated periodic data processing, enabling structured insights for analysis.
