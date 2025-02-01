## Project Overview
This project involves building a scalable ETL (Extract, Transform, Load) pipeline for GoldFintech to efficiently process financial data, enabling data-driven decision-making. The pipeline integrates data from the Alpha Vantage API into a structured and actionable format, using a combination of Azure services and Databricks to ensure seamless data flow, transformation, and storage.

### Architecture
The ETL pipeline consists of the following key components:
![image](https://github.com/user-attachments/assets/a165b792-bf1e-4f1b-9939-1d76daa25552)

### 🔹 Data Ingestion
Azure Data Factory ingests data from the Alpha Vantage API.
The data is stored in raw format in Azure Blob Storage.
### 🔹 Data Transformation
Databricks processes the raw data.
Transformations are applied to clean and structure the data.
The cleaned data is saved back to Azure Blob Storage.
### 🔹 Data Analysis
Azure Synapse Analytics retrieves the cleaned data.
The transformed data is stored in an SQL Synapse database.
Enables advanced analytics and reporting.
### 🔹 Automation
The entire ETL process is triggered to execute automatically on a weekly basis.
### Technologies Used
Azure Data Factory – For data ingestion
Azure Blob Storage – For raw and processed data storage
Databricks – For data transformation and processing
Azure Synapse Analytics – For data warehousing and advanced analytics
Alpha Vantage API – As the external data source
