# Data Pipeline: Automatization with Apache Airflow
Project completed as part of the Data Engineering Nanodegree Program

## Sparkify

**Sparkify:** Sparkify is a fictive company with a music streaming mobile application.

**Analytics Goals:** Sparkify wants to introduce more automation and monitoring to their data warehouse ETL pipelines. 

**Initial Data:** Data collected by Sparkify are initially stored into two types of JSON files on S3: 

1. The user activity on the app (will be refered as: *log_data* or *events_data*), which contains data specific to each user and his associated activities
2. Metadata on the songs in the app (will be refered as: *song_data*), which contains information about specific songs and artists

**Objectives:** The objective is to develop a pipeline for Sparkify that will automaticly take the raw data from its sources, transform them into a new star schema (see graph below) and store the new data in the cloud (in this case: redshift). The goal is to create custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

![Schema of the relational database - Data Modeling](pipeline_dags.PNG)

## Data Pipeline Project

### Steps to run the project

1. Create role and redshift cluster in AWS

2. Create empty tables in redshift to initilize the reception of the data and creation of the tables

3. Launch Apacha Airflow and run the Dag

### Files in this project

*Notes: The followings files requires to have an AWS access, a roles allowing S3 reads, an active redshift bucket to run and connection to Apache Airflow*

- *create_tables.sql* : this file contains the SQL statement use to create the empty table in redshift

**Dag**
- *sparkify_pipeline.py* : this is the main file that will run. It starts the dag, add all the operator that will be run and define the order in which the pipeline should process

**Plugins**
Operators : contains all the cutom operators created for this project.
- *stage_redshift.py*: Custom operator that take data from S3 and load the raw data into redshift (required staging tables to be created prior this step)
- *load_fact.py*:  Cutom operator that take data from redshift and apply a SQL statement to create a new fact table (required fact table to be created prior this step)
- *load_dimension.py*: Cutom operator that take data from redshift and apply a SQL statement to create a dimension table (required dimensiontable to be created prior this step)
- *data_quality.py* : Custom operator that check if the value returned from a SQL statement correspond to the expected value. 
	-  Can check 1 or more querries (list of querries)
	-  Raise a "skip" action in Airflow if no querry was given
	-  Raise an error (after retrying 3x) if one expected value is not the same as the result of the querry
	-  Marked as "success" if ALL the given querries worked
- *empty_quality.py*: Custom operator that check if a table is empty
	- Receive a list of 1 or more tables name to check if they are empty
	- Raise a "skip" action in Airflow if no table is given to check
	- Raise an error if one table is empty
	- Marked success if all tables have data

Helpers
- *sql_qurries.py*: this file contains all the querries used to transform the data 

## Project Improvement
Here's a list of the thing I would like to improve (having more time to put!)
- Coding an operator that create the table directly in the pipeline if the table does not exist yet

- Creating a SubDAGs to check data quality (check if table are empty and run quality checks in parallel). So that it's less messy in the diagram, easier to add/remove/modify steps in the quality check. 