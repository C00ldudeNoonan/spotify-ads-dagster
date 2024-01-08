{{
    config(
        materialized='table'
    )
}}



{% set table_name = 'linden-data-warehouse.spotify_ads.daily_ad_history' %}
{% set relation = adapter.get_relation(database=table_name.split('.')[0], schema=table_name.split('.')[1], identifier=table_name.split('.')[2]) %}

{% if relation %}
WITH start_date AS (
  SELECT 
    MIN(cast(left(start_time,10) as date)) as start_date
  FROM {{ source('spotify_ads', 'incremental_refresh_ad_stats_daily') }}
  
),


full_history AS (
  SELECT 
    ad.*
  FROM `linden-data-warehouse.spotify_ads.daily_ad_history` ad  
  JOIN start_date
  ON cast(left(ad.start_time,10) as date) <= start_date.start_date
),

incremental_refresh AS (
  SELECT
    * 
  FROM {{ source('spotify_ads', 'incremental_refresh_ad_stats_daily') }}
)

SELECT * FROM full_history
UNION DISTINCT
SELECT * FROM incremental_refresh

{% else %}
WITH start_date AS (
  SELECT 
    MIN(cast(left(start_time,10) as date)) as start_date
  FROM {{ source('spotify_ads', 'incremental_refresh_ad_stats_daily') }}
  
),


full_history AS (
  SELECT 
    ad.*
  FROM `linden-data-warehouse.spotify_ads.full_history_ad_stats_daily` ad  
  JOIN start_date
  ON cast(left(ad.start_time,10) as date) <= start_date.start_date
),

incremental_refresh AS (
  SELECT
    * 
  FROM {{ source('spotify_ads', 'incremental_refresh_ad_stats_daily') }}
)


SELECT * FROM full_history
UNION DISTINCT
SELECT * FROM incremental_refresh
{% endif %}
