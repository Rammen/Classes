# Data Modeling with Apache Cassandra 
Project completed as part of the Data Engineering Nanodegree Program

## Sparkify

**Sparkify:** Sparkify is a fictive company with a music streaming mobile application.

**Analytics Goals:** Sparkify wants to analyze songs' and user's activities from their new music streaming application. The analysis team is particularly interested in understanding what songs users are listening to. The analytics team want to perform the three following queries:

- Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
- Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
- Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

**Initial Data**: Data collected by Sparkify are initially stored into a csv file.

## Projects Files

### Jupyter
In this project, I used two Jupyter files (.ipynb):
- Final_Project: 
    - Section 1: Denormalization of the data
    - Section 2: Create an Apache Cassandra database, load the data into tables and run specific queries to ensure the data were well inserted
- Data_Exploration : 
    - Each section (Query 1, Query 2 and Query 3) described the process behind how I chose the primary key. Since no informations were given about the dataset, I used analytics with Pandas to better understand the dataset.

Dataset:
- Raw data are not included, thus section 1 of Final Project can not run
- The CSV output of section 1 is included (event_datafile_new.csv)

## How to run this project

To run this project:
1. Run all cells from Final_Project (you can omit section 1 since raw data are not included)