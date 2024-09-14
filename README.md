**Data Pipeline Project**

Overview

This project is a comprehensive data pipeline that encompasses the following key steps:

Data Crawling from IMDb:
Utilizes tools and libraries such as BeautifulSoup or Scrapy to scrape data from the IMDb website (i use selenium)
Processes and cleans the data to prepare it for subsequent steps.

Loading Data into Google BigQuery:
The collected data is loaded into Google BigQuery, a cloud-based data warehousing service.
Uses the google-cloud-bigquery library to handle data uploads and manage BigQuery table schemas.

Data Analysis:
Executes SQL queries to analyze the data within BigQuery.
Generates reports and visualizations to facilitate understanding and decision-making.


Next development: 
Automation with Apache Airflow
Sets up Apache Airflow to automate the entire data pipeline process.
Complete Directed Acyclic Graphs (DAGs) to define workflows, from data collection and loading to analysis and reporting.
Configures schedules for task automation and monitors their execution.


