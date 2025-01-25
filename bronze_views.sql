-- create database if not exists me9hanics_homework;

DROP TABLE IF EXISTS me9hanics_homework.bronze_views;

CREATE EXTERNAL TABLE
me9hanics_homework.bronze_views (
    article STRING,
    views INT,
    rank INT,
    date DATE,
    retrieved_at TIMESTAMP)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://de2-mihaly-wikidata/datalake/views/'; -- retrieved_at could be string too