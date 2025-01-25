DROP TABLE IF EXISTS me9hanics_homework.silver_views;

CREATE TABLE me9hanics_homework.silver_views
WITH (
      format = 'PARQUET',
      parquet_compression = 'SNAPPY',
      external_location = 's3://de2-mihaly-wikidata/datalake/views_silver/'
) AS 
SELECT article,
        views,
        rank,
        date 
FROM me9hanics_homework.bronze_views;