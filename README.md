 Project Summary
 ---
**Introduction**
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

To achieve the analytics request a Postgres database will be utilized with a star schema (i.e., fact and dimension tables) to model data and a ETL pipline will transfer that data from files in two local directories into these tables in Postgres using Python and SQL.
 
 Running Python Scripts
 ---
 To execute the ETL process of Extracting, Transforming, and Loading local data into the Sparkify database in Postgres execute the scripts as outlined below:
 
Order to run Python scripts: **create_tables.py** --> **etl.py**

 
 Repository Files
 ---
 * sql_queries.py : contains all PostgreSQL queries (i.e., dropping, creating and inserting data into tables in Sparkify database)
 * create_tables.py : creates sparkify database and all fact & dimension tables
 * etl.py : Extracts, Transforms, and Loads data into fact & dimension tables
 * etl.ipynb : Interactive notebook guiding the development of the ETL process for etl.py
 * test.ipynb : Interactive notebook that can be used to view fact & dimension tables with loaded data
 * README.md