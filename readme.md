# Project Title
### Data Engineering Capstone Project

#### Project Summary

#### Introduction

Oakland Department of saftey has data that has been collected over the time. Their data resides in S3, in csv files with records-for-YYYY.csv.

As their data engineer, i have been assigned in building etl pipelines that extract their data from s3, process them using spark and loads the data back into s3 in parquet format. The final transformed data in facts and dimensions will be pushed in to Amazon Redshift datawarehouse, where all analytical team will be interacting and trying to find out some insights that may effectively place resources in the areas where there is high crime activity.

#### Step 1:

#### Project Description

In this project, I have applied what i have learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3.
Also I have used my best ability to load the data into amazon redshift database where the actual facts and dimensions data will be resided as my final target for the analytics team to use that for their effective analysis to work towords improving protection for oakland citizens.


To complete the project, i have loaded data from S3, process the data into analytics tables using Spark, and load them back into S3 in parquet format. Final target destination is amazon redshift. 

#### Project Datasets

Four datasets that reside in S3. I have attached the csv sheets in the local workspace and used that as source.

* records-for-2011.csv
* records-for-2013.csv
* records-for-2015.csv
* records-for-2016.csv 

#### Step 2:

#### Incidents Datasets

All the data that is being in four csv sheets are related to incidents that are happend over the years.

#### Schema for Incidents 

* Agency
* Create Time
* Location
* Area Id
* Beat
* Priority
* Incident Type Id
* Incident Type Description
* Event Number
* Closed Time



#### Step 3:

#### Dimensional Modelling:
A dimensional model is also commonly called a star schema.The core of the star schema model is built from fact tables and dimension tables. It consists, typically, of a large table of facts (known as a fact table), with a number of other tables surrounding it that contain descriptive data, called dimensions. 

#### Fact Table:
The fact table contains numerical values of what you measure. Each fact table contains the keys to associated dimension tables. Fact tables have a large number of rows.The information in a fact table has characteristics. It is numerical and used to generate aggregates and summaries. All facts refer directly to the dimension keys. Fact table that is determined after carefull analysis which contains the information.

#### Tables (Facts)
Table Name: fact_incidents(fact)
Column Names: 
* incidents_id
* create_time
* start_time
* end_time 
* agency
* location 
* area_id
* beat
* priority_key 
* incident_type_id 
* eventnumber 
* year
* month 

#### Dimension Tables:
The dimension tables contain descriptive information about the numerical values in the fact table. 

#### Tables ( Dimensions )

Table Name:incidents  (incdents Dimension)
Column Names: incident_type_id, incident_description 

Table Name: priority (priority Dimension)
Column Names: priority_key , priority_level varchar

Table Name: Time (Time Dimension)
Column Names: start_time, hour, day, week, month, year, weekday


#### Step 4:

#### ETL Pipeline
ETL pipeline consists of three steps. Please refer functions from my script 
* Extraction -- This has been carried out by function extract_df
* Transformation -- This has been carried out by function transform_df
* Load -- This has been carried out by function load_df 

The data is extracted from extract function which returns the dataframe. This dataframe is the input for the transform_df function to get the appropriate transformations and then load them as parquet files in s3 which are later copied to amazon redshift cloudwarehouse for analytical team to access them for further analysis.

#### Pre-requisites cluster setup:
An Amazon redhift cluser should be created and able to connect when using any tool. I have used Aginity workbench. Place the host name and other parameters in the dwh.cfg file. 

I have creates 3 dc2.Large instances to get my etl script running

#### Database creation and loading Approach: 
Implement all the database DDL and DML statements in the rs_queries.py that create all the dimension tables and fact tables with all necessary columns. Build an ETL pipeline to extract data from the dimensionally modelled parquet files from s3 and  push the data into necessary dimensions and fact tables. 


#### Step 5:

All the steps from 1 to 4 covers all my project in detail. In this step i am including the analysis overview on my data. 

These are some of the assumed analysis as per my knowledge so far, even though these may not be the carved stone scenarios, the point is data is available for the analysts to use to improve peacefulness of oakland citizens.

Analyis 1: to find top 10 crime incidents 
output: Please refer to the Analysis_1.csv

select incident_description, count(fi.incident_type_id) incident_count
from fact_incidents fi 
join incidents i on fi.incident_type_id = i.incident_type_id
group by incident_description
order by 2 desc
limit 10;


Aim: Analysis of incidents description count by priority for each incident description
output: Please refer to the Analysis_2.csv


select incident_description, priority_level,count(fi.incident_type_id) incident_count
from fact_incidents fi 
join incidents i on fi.incident_type_id = i.incident_type_id
join priority p on fi.priority_key = p.priority_key
group by incident_description,priority_level
order by 2,3 desc
limit 10;


Aim: Analysis of incident counts by year and by priority 
output: Please refer to the Analysis_3.csv

select t.year, priority_level,count(fi.incident_type_id) incident_count
from fact_incidents fi 
join priority p on fi.priority_key = p.priority_key
join time t on fi.create_time = t.create_time
group by t.year,p.priority_level
order by 1,2,3 desc;


### List of scripts attached 

#### Data Source 
* records-for-2011.csv
* records-for-2013.csv
* records-for-2015.csv
* records-for-2016.csv 

#### Config parameters in dwh.cfg
* CLUSTER details
* AWS Details 
* Parquet files location
* Iam role details

#### All my queries are in rs_queries.py
* Drop queries 
* Create queries
* Insert queries 
* Copy queries 
* data quality queries

### Setup of facts and dimension tables in redshift. 
* create_tables.py 
* redshift_setup() -- to drop and create tables which is done only once before executing the etl script assuming the cluster is already setup


### ETL script in etl.py 

#### Prep work of necesary connections related functions
* get_spark() -- to get the spark object 
* get_config() -- to read any config file once and pass the config object 
* get_connection() - to get the redshift connection based on cluster info
* get_aws_credentials() - to get the aws key and secret



#### Actual ETL script in action functions
* extract_df() -- to extract data from the source and returns the dataframe
* transform_df() -- to clean and load data into s3 parquet files
* load_df() -- to load from paruqet files into s3 which is called from transform_df() 



#### Parquet files loaded into cloud datawarehouse. 
* redshift_copy() -- to load the parquet files into cloudwarehouse and get the list of counts after loading all the data which is used in data quality check function


#### Data Quality Check operation 
* data_quality_check() --  Gets the list of counts from transform_df() function and also get the finally loaded table counts from the redshift_copy() functions and compares the the lists. 


If the function returns below 
* "counts matched, quality check passsed" -- all data is loaded as expected
* "counts did not match, quality check failed" -- all data is not loaded as expected



