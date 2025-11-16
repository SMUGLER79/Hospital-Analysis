# Hospital-Analysis
This project showcases a real-time healthcare data engineering pipeline that tracks patient movement across hospital departments using Azure services. Streaming data is captured, processed in Databricks with PySpark, and loaded into Azure Synapse SQL Pool for reporting and analytics.

## Tools & Technologies Used
  * Azure Event Hub
  * Azure Databricks
  * Azure Data Lake Storage
  * Azure Synapse SQL Pool
  * Power BI
  * Python

## Workflow
### Event Hub Setup
Created namespace and patient-flow hub with consumer groups for streaming into Databricks.

### Data Simulation
Built patient_flow_generator.py to continuously send synthetic patient data (department, wait time, discharge status) to Event Hub.

### Storage Configuration
Set up ADLS Gen2 with bronze, silver, and gold containers.

### Databricks Processing
Notebook 1: Ingests Event Hub stream into Bronze.
Notebook 2: Cleans and standardizes schema.
Notebook 3: Aggregates data and builds star-schema tables.

### Synapse SQL Pool
Provisioned dedicated SQL Pool and executed DDL scripts for fact/dimension tables.


## Data Analytics
Used PowerBI to build an interactive dashboard for analysis and insights.

<img width="1282" height="724" alt="image" src="https://github.com/user-attachments/assets/893706a7-5caf-4336-ad0f-afc4d809c1a1" />

Synapse SQL Pool

Provisioned dedicated SQL Pool and executed DDL scripts for fact/dimension tables.
