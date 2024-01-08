{{
    config(
        materialized='view'
    )
}}
SELECT 
id as campaign_id
,Name as campaign_name
,cast(left(CREATED_AT,10)  as date) as created_date
,cast(left(UPDATED_AT,10) as date ) as updated_date
,Status as status
,OBJECTIVE as objective
, account_id as account_id
FROM {{ source('spotify_ads', 'campaigns') }}