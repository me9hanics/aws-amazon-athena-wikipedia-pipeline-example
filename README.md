# aws-amazon-athena-wikipedia-pipeline-example
Simple AWS pipeline calling the Wikipedia API for top daily pageviews, then uploading processed data to AWS S3 in a medallion architecture style data warehouse - running SQL scripts in Amazon Athena to create the layers.

The `__main__` function of `extract_views.py` collects the top daily pageviews from 4 consecutive days (2024/11/15-18), and stores it in an AWS S3 bucket (data storage).<br>
The separate (bronze/silver/gold) "views" files are SQL scripts intended to be run in Amazon Athena, creating separate data tables (representing the medallion architecture, alias delta lakes). The bronze layer stores in a table all data as is, the silver layer converts it into Parquet format and removes a redundant column, while the gold layer script takes the silver layer table and aggregates daily instances in a compact form (storing only the total aggregated views for each top article across every day ingested into the data lake).
