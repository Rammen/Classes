# Data Modeling with Postgres
Project completed as part of the Data Engineering Nanodegree Program

## Sparkify

**Sparkify:** Sparkify is a fictive company with a music streaming mobile application.

**Analytics Goals:** Sparkify required a new ETL pipeline to congregate their user's data. This will allow their Analytics Team to better understand what songs are listened by their users. Their analytics team will then use their own querries. 

**Initial Data:** Data collected by Sparkify are initially stored into two types of JSON files: 

1. The user activity on the app (will be refered as: *log_data*), which contains data specific to each user and his associated activities
2. Metadata on the songs in the app (will be refered as: *song_data*), which contains information about specific songs and artists

## Data Modeling

In this project, we created 1 fact table and 4 dimension tables:

**Fact Table** : 
- *Songplays* : records in log_data associated with song plays

**Dimension Tables**: Information about either the song or the user:
- *users* : information about each users in the app
- *songs* : information about each songs in the music database
- *artists* : artists in the music database
- *time* : timestamps of records in songplays broken down into specific units

![Schema of the relational database - Data Modeling](https://github.com/Rammen/Classes/tree/main/DE_Nanodegree_Projects/DataModeling_PostgreSQL/Data_Modeling.PNG)

## Projects Files

### Jupyter
In this project, we used Jupyter files (.ipynb) to develop the process behind the pipeline and test on a subset of the data. Those files are:

- *test.ipynb* is a short notebook that launches PostgreSQL and displays the first five rows of each table to ensure that everything works properly
- *etl.ipynb* is the main notebook used to develop the ETL pipeline. It contains steps and notes from Udacity to develop the ETL pipeline correctly. In this notebook, we use only one file from the log_data and one from the song_data to create and model our relational database. 
- *sandbox_observation.ipynb* is a file that I created to explore the data and observe columns, data types, etc. 

### Python
The final project runs via python files. We have three files:

- *create_tables.py* can be run and will 1) connect to the database, 2) delete the database and tables if they exist and 3) create five new tables based on the schema describe and shown above. 
- *etl.py* can be run and will 1) connect to the database and 2) load each table with the data from JSON files
- *sql_queries.py* is a file that contains all the queries used in this project


## How to run this project

To run this project:
1. Launch the file *create_tables.py* to create the database and tables for this project
2. Launch the file *etl.py* to add data into the tables
