{{
    config(
        materialized='view'
    )
}}

SELECT 
ID as ad_set_id
,Name as ad_set_name
,CAST(LEFT(START_TIME,10) as date) as ad_set_start_date
,CAST(LEFT(END_TIME,10) as date) as ad_set_end_date
,BID_MICRO_AMOUNT as ad_set_bid_micro_amount
,DELIVERY as ad_set_delivery
,CATEGORY as ad_set_category
,CAMPAIGN_ID as campaign_id
,COST_MODEL as ad_set_cost_model
,CAST(LEFT(CREATED_AT, 10) as date) as ad_set_created_date  
, CAST(LEFT(UPDATED_AT, 10) as date) as ad_set_updated_date
, ASSET_FORMAT as ad_set_asset_format
, PROMOTION as ad_set_promotion
, BID_STRATEGY as ad_set_bid_strategy
, REJECT_REASON as ad_set_reject_reason
, STATUS as ad_set_status
, PACING as ad_set_pacing
, BUDGET_MICRO_AMOUNT as ad_set_budget_micro_amount
, BUDGET_TYPE as ad_set_budget_type
, TARGETS_ARTIST_IDS ad_set_targets_artist_id
, TARGETS_GEO_TARGETS_COUNTRY_CODE as ad_set_target_county_code
, TARGETS_GEO_TARGETS_CITY_IDS ad_set_target_city_id
, TARGETS_GEO_TARGETS_DMA_IDS ad_set_target_dma_id
FROM {{ source('spotify_ads', 'ad_sets') }}