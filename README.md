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

# Table View List from Amazon Redshift

Tables are created under the Public schema inside the “Redshift-cluster-1”

![image](https://github.com/aseemnarula1/Udacity_Project_Data_Pipelines_With_Airflow/assets/48493330/5e51799b-f138-4a57-bddd-3724c3b14da1)

## Building the DAG operators
I have used the Airflow’s built-in functionalities as connections and hooks as much as possible and let Airflow do all the heavy lifting when it is possible.

All of the operators and task instances will run SQL statements against the Redshift database. However, using parameters wisely have allowed me to build flexible, reusable, and configurable operators many kinds of data pipelines with Redshift and with other databases.

## Stage Operator
The stage operator is expected to be able to load any JSON formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The operator’s parameters should specify where in S3 the file is loaded and what is the target table.

The parameters should be used to distinguish between JSON file. Another important requirement of the stage operator is containing a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills.


## Fact and Dimension Operators
With dimension and fact operators, I have utilized the provided SQL helper class to run data transformations. Most of the logic is within the SQL transformations and the operator is expected to take as input a SQL statement and target database on which to run the query against. I have also defined a target table that will contain the results of the transformation.

Dimension loads are often done with the truncate-insert pattern where the target table is emptied before the load. Thus, I also have a parameter that allows switching between insert modes when loading dimensions. Fact tables are usually so massive that they should only allow append type functionality.

## Data Quality Operator
The final operator to create is the data quality operator, which is used to run checks on the data itself. The operator’s main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each the test, the test result and expected result needs to be checked and if there is no match, the operator should raise an exception and the task should retry and fail.

## Final Data Quality Checks with AWS Redshift Query Editor

I have double checked and cross verified the results the AWS Redshift Query Editor with the Airflow Web UI Info logs at the run time.
![image](https://github.com/aseemnarula1/Udacity_Project_Data_Pipelines_With_Airflow/assets/48493330/3d590ab7-5b8f-4528-840b-844d9f5ac04f)

## Staging Events Row Count
![image](https://github.com/aseemnarula1/Udacity_Project_Data_Pipelines_With_Airflow/assets/48493330/bb030f44-5cca-4a87-b5d1-292ca0b42249)


## Staging Songs Row Count
![image](https://github.com/aseemnarula1/Udacity_Project_Data_Pipelines_With_Airflow/assets/48493330/f4ae1471-2e60-4261-91b8-b5f1e729cc59)


## Songplays Row Count
![image](https://github.com/aseemnarula1/Udacity_Project_Data_Pipelines_With_Airflow/assets/48493330/96359c40-9b25-4349-8d53-25b02b6d83a4)


## Users Table Row Count
![image](https://github.com/aseemnarula1/Udacity_Project_Data_Pipelines_With_Airflow/assets/48493330/5c56f9f9-e036-4f29-8d26-832051aa222c)


## Songs Row Count
![image](https://github.com/aseemnarula1/Udacity_Project_Data_Pipelines_With_Airflow/assets/48493330/d7d139f6-cebc-4e51-a229-df05cd3afca1)


## Time Table Row Count
![image](https://github.com/aseemnarula1/Udacity_Project_Data_Pipelines_With_Airflow/assets/48493330/405df0b6-d656-48e0-a842-34461fa3c25c)


## Artists Row Count
![image](https://github.com/aseemnarula1/Udacity_Project_Data_Pipelines_With_Airflow/assets/48493330/b70acfe2-ba00-41ea-bf30-1612f92a56e5)


## Reference Links —

1. Airflow Base Operator — https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/baseoperator/index.html
2. Airflow Postgres Hook — https://hevodata.com/learn/airflow-hooks/
3. S3 COPY Command — https://docs.aws.amazon.com/redshift/latest/dg/t_loading-tables-from-s3.html
4. Stage to RedShift Operator — https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/transfer/s3_to_redshift.html
5. Airflow DAG Default Arguments — https://airflow.apache.org/docs/apache-airflow/2.0.1/_modules/airflow/example_dags/tutorial.html
6. Airflow Stack Overflow DAG Fields — https://stackoverflow.com/questions/71983633/different-way-to-set-airflow-dag-fields
7. Airflow DAG Logging Stackoverflow — https://stackoverflow.com/questions/40120474/writing-to-airflow-logs


## Acknowledgement

All the datasets of Sparkify used in this Data Engineer Project are provided through Udacity and are used for my project with Udacity Data Engineer Nanodegree and reference links are also provided where the docs are referred.






