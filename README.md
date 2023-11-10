# Udacity_Project_Data_Pipelines_With_Airflow
Udacity Project: Data Pipelines with Airflow

# Introduction
Project: Data Pipelines with Airflow
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify’s data warehouse in Amazon Redshift. The source datasets consist of JSON logs that talk about user activity in the application and JSON metadata about the songs the users listen to.

# Project Overview
This project needs us to use the core concepts of Apache Airflow. To complete this project, I will need to create my own custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

Following is the screenshot for Sparkify Udacity DAG
![image](https://github.com/aseemnarula1/Udacity_Project_Data_Pipelines_With_Airflow/assets/48493330/40472382-89c8-4d38-b8cf-13d0fa0cd438)


