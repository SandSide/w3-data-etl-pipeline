# W3 Data ETL Pipeline

## Overview
This project consisted of creating an ETL pipeline via Apache Airflow to process W3 log data.

The data was transformed into a Star Schema, consisting of single transactional fact table and multiple dimensional tables.

The Star Schema was then loaded into PowerBI to be used for exploratory data analysis and data visualization.

## Extraction
The data consisted of multiple log files capturing web traffic activity of a website.


## Transformations
- Aggregation - Combining multiple log files into a single source.
- Bot Detection - Determining whether a request was made by a bot or not.
- IP Location - Determining location of a request based on IP address.
- OS/Browser/Device - Determining os, browser and device details of a request based on the browser/user agent string.
- File Path Details - Deriving file path details, such as file name, directory, file extension and file type based on raw file path string. The details were also cleaned to get rid of unnecessary details (e.g. query string), and bad characters.
- Status Codes - Classifying and deriving additional data from status codes.
- Time Taken - Grouping time taken into predefined ranges.

## Dimensional Tables
The final star schema consisted of the following dimensional tables:
- Time
- Date
- IP
- File
- Browser
- OS
- Device
- Time Taken
- Status Code
- Http Method