# DAEN-328-Term-Project
In this project we build a full working pipeline that gathers transportation data from New York city. The aim of this is to understand and see which transportations are the most popular in the city


Steps:

1) Ingesting + data cleaning - AJ
2) ERD - Altar
3) Pipeline to postgress - Sreesh
5) Visualization (Looker/Streamlit...) - Altar
6) Presentation

1. Setup a data pipeline to download the initial data set through REST API.
2. Develop a set of cleaning functions for the data set. Each function needs to take care of one small cleaning function, e.g., normalizing capitalization.
3. Clean the data set and save cleaned data either in original form or in SQLite.
4. Develop a database schema for the data set that eliminates redundancy, ensures data integrity, and simplifies querying. 
5. Create a database using this schema and port the data into it.
6. Develop a web app dashboard for visualizing the database and running read only queries.
7. Develop a batch-oriented database update mechanism that brings in new data from the data source.
8. Demonstrate the project with a in class presentation.
