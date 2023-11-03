## Overview

The goal of this project is to perform a data engineer ETL pipeline for creating a Data Warehouse on the Brazilian job market dataset named RAIS, using technologies such as Airflow, Pyspark with Azure Databricks, Azure Synapses Analytics, and Power BI. 
### Data Visualization

![Example dashboard image](Rais_dashboard.png)

### Data Architecture

![Example architecture image](rais-data-architecture.drawio.png)

- The original dataset was a 6-part file, separated by Brazilian regions.
- Extract and compress the dataset file to a gzip format locally with Airflow, so we have a light file size to upload to blob storage and a compatible file format to spark to read.
- Load data in Azure blob storage or Azure Data Lake so we can easily interact with Databricks and Azure Synapses Analytics.
- The unified dataset has more than 65 million records so we need powerful data processing like spark.
- Azure Synapses Analytics provides an easy-to-set external table from Azure Data Lake Storage Gen 2 acting like a Data Warehousing.
- Azure Synapses Analytics also provides an interface with Microsoft Power BI, through a linked service configuration, however, due to my Microsoft account not being of a Professional type I have to plug it into the Microsoft BI desktop. 

### Dataset
The RAIS job market dataset records include fields capturing the month of admission, salary, age, hours worked within a month, sex, color race, type of occupation, city, state, industry sector and subsector, duration of actual labor, and much more.

Here we have information about the dataset used in this project and how to download it- https://www.gov.br/trabalho-e-emprego/pt-br/assuntos/estatisticas-trabalho/microdados-rais-e-caged

## Prerequisites

Directions or anything needed before running the project.

- Prerequisite 1
- Prerequisite 2
- Prerequisite 3

## How to Run This Project

Replace the example step-by-step instructions with your own.

1. Install x packages
2. Run command: `python x`
3. Make sure it's running properly by checking z
4. To clean up at the end, run script: `python cleanup.py`

## Lessons Learned

It's good to reflect on what you learned throughout the process of building this project. Here you might discuss what you would have done differently if you had more time/money/data. Did you end up choosing the right tools or would you try something else next time?

## Contact

Please feel free to contact me if you have any questions at: LinkedIn, Twitter
