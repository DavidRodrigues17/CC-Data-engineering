# Azure Data Engineering Project

## Overview

This project aims to provide a comprehensive learning experience in Azure Data Engineering. It covers essential Azure tools and technologies, including Azure Data Factory, Azure Databricks, and Azure Synapse Analytics, through real-world scenarios and practical implementations. The project is designed to help you build a solid foundation in data engineering and prepare for technical interviews.

## Project Structure

The project follows a Medallion architecture, which consists of three layers:

- **Bronze Layer**: Raw data is ingested from various sources without any transformation.
- **Silver Layer**: Data is cleaned and transformed to ensure quality and consistency.
- **Gold Layer**: Data is aggregated and prepared for analysis and reporting.

## Tools and Technologies

- **Azure Data Factory (ADF)**: For orchestrating data workflows and moving data between services.
- **Azure Databricks**: For data processing and transformation using Apache Spark.
- **Azure Synapse Analytics**: For data warehousing and serving data to stakeholders.
- **Power BI**: For data visualization and reporting (briefly covered).
- **Azure Blob Storage and Containers**: For managing and organizing raw data storage.

## Getting Started

### Prerequisites

- An Azure account (you can create a free account [here](https://azure.microsoft.com/free/)).
- Basic understanding of data engineering concepts.

### Setup Instructions

1. **Create an Azure Account**: Sign up for a free Azure account to access the necessary services.
2. **Create Resource Group**: Set up a resource group in Azure to organize your resources.
3. **Create Storage Account**: Create a storage account to store raw data.
4. **Create Containers**: Organize data into containers within the storage account to manage access and structure effectively.
5. **Create Data Factory**: Set up Azure Data Factory to orchestrate data workflows.

### Project Steps

1. **Data Ingestion**:
   - Create a static pipeline in Azure Data Factory to load data from a GitHub repository into the Bronze layer.
   - Using linked services to establish connections between data sources and destinations.
   - Define datasets to specify the structure of the data being moved.

2. **Data Transformation**:
   - Using Azure Databricks to clean and transform the data in the Silver layer.
   - Implement data quality checks and transformations to ensure data integrity.

3. **Data Aggregation**:
   - Prepare the data in the Gold layer for analysis and reporting.
   - Using Azure Synapse Analytics to create a data warehouse for serving data to stakeholders.

4. **Data Visualization**:
   - Using Power BI to create visualizations and reports based on the processed data.

## Real-Time Scenarios

This project includes real-time scenarios, such as:

- Building dynamic pipelines in Azure Data Factory.
- Handling data transformations in Azure Databricks.
- Understanding data redundancy and storage options in Azure.
- Using Azure Containers for structured data storage.

## Screenshots

Below, paste the relevant screenshots for each step:

1. **Resource Group Setup**: ![Resource group](https://github.com/user-attachments/assets/67767775-ba14-4862-aa99-d12b41e1cca4)
2. **Azure Containers**: ![containers](https://github.com/user-attachments/assets/4be94a61-2ea3-4515-bee8-4aad29cb738e)
3. **Data Ingestion using Data Factory**: ![bronze](https://github.com/user-attachments/assets/0570762b-ca1e-466a-8e83-f8d69d7ffc7e)
4. **Python in Databricks (Silver Layer)**: ![Silver](https://github.com/user-attachments/assets/49c95657-65d8-4068-856f-7606d69298c6)
5. **Two Visuals from Silver Layer**:![visual1](https://github.com/user-attachments/assets/76bb227e-3f13-4445-a3fa-227f57fdb4cc) <br/>![visual2](https://github.com/user-attachments/assets/623c554f-f3d6-4cfe-89fb-681c77950689)

6. **Views in Synapse Analytics**: ![gold](https://github.com/user-attachments/assets/1cfac73b-69e9-43d5-bc14-2a44e7eec512)
7. **Power BI Connection**: ![con](https://github.com/user-attachments/assets/2b8a2879-9a5a-4c54-aa64-c6f0dc4ebf0e)

8. **Power BI Visualization**: ![BI1](https://github.com/user-attachments/assets/25b605be-d36d-4b95-80e9-584a843c7230) <br/> ![Bi2](https://github.com/user-attachments/assets/45831f3f-625b-416c-9d96-d41e8111bfdf)

