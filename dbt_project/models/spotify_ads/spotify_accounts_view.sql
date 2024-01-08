{{
    config(
        materialized='view'
    )
}}
SELECT
id as account_id
,cast(left(CREATED_AT,10) as date) as created_date
,cast(left(UPDATED_AT,10) as date) as updated_date
,COUNTRY_CODE as country
,industry
,name as account_name
FROM {{ source('spotify_ads', 'accounts') }}
