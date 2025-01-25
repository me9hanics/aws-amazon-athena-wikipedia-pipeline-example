DROP TABLE IF EXISTS me9hanics_homework.gold_allviews;

CREATE TABLE me9hanics_homework.gold_allviews 
WITH (
    format = 'PARQUET',
    parquet_compression = 'SNAPPY',
    external_location = 's3://de2-mihaly-wikidata/datalake/gold_allviews/'
) AS
SELECT 
    article,
    SUM(views) AS total_top_view,
    MIN(rank)  AS top_rank, 
    COUNT(date) AS ranked_days
FROM 
    me9hanics_homework.silver_views
    
GROUP BY 
    article
ORDER BY
    total_top_view DESC;
    
-- SELECT * FROM "me9hanics_homework"."gold_allviews" limit 100 ;