## Project Summary

Data project for processing and analyzing Olist ecommerce dataset (https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce?select=olist_customers_dataset.csv). 

This project processes and analyzes the Olist e-commerce dataset through an automated AWS data pipeline. Apache Airflow orchestrates the workflow, using AWS Glue (PySpark) for data cleaning, Boto3 for S3 storage management, and Amazon Athena for SQL transformations. 

The final data is visualized in a Power BI dashboard to deliver business insights.

## Pipeline Architecture

![Pipeline Diagram](screenshots/olist_pipeline_diagram.png)

Apache Airflow orchestrates a Medallion data lake architecture on AWS. Raw CSVs (Bronze) are transformed via AWS Glue PySpark into optimized Parquet files (Silver), which are then queried by Amazon Athena to create final business-level aggregations (Gold) for Power BI.

## Dashboard

Built in Power BI Desktop connecting to AWS Athena query results stored in Amazon S3. 
The published dashboard uses a static, imported dataset. 

### Revenue Overview
![Revenue & Sales Performance](screenshots/dashboard_page_1.jpg)

### Payment Breakdown
![Payment Methods Analysis](screenshots/dashboard_page_2.jpg)

### Customer Analysis
![Customers](screenshots/dashboard_page_3.jpg)

### Delivery Analysis
![Delivery Analysis](screenshots/dashboard_page_4.jpg)

### Category Analysis
![Categories](screenshots/dashboard_page_5.jpg)

[Download .pbix file](dashboard/olist_dashboard.pbix)
