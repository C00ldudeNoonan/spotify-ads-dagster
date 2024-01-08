{{
    config(
        materialized='view'
    )
}}

SELECT  
id as ad_id
,name as ad_name
,ADVERTISER_NAME as account_name
,tagline as tagline
,cast(left(CREATED_AT,10)  as date) as created_date
,cast(left(UPDATED_AT,10) as date ) as updated_date
,Delivery as  ad_delivery
,ad_set_id as ad_set_id
,status as ad_status
,AD_PREVIEW_URL as preview_url
,ASSETS_ASSET_ID as asset_id
,CALL_TO_ACTION_TEXT as call_to_action_text
,CALL_TO_ACTION_CLICKTHROUGH_URL as cta_url
,ACCOUNT_ID as ad_account_id
FROM {{ source('spotify_ads', 'ads') }}