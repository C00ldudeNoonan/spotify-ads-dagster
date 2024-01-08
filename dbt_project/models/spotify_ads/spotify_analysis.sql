{{
    config(
        materialized='view'
    )
}}

SELECT 
ENTITY_ID as ad_id
,cast(left(START_TIME, 10) as date) as reporting_date
,entity_name as ad_name
,CLICKS as clicks
,COMPLETES as completes
,CTR as click_through_rate
,E_CPCL as effective_cost_per_listen
,E_CPM as effective_cost_per_mile
,FIRST_QUARTILES as first_quartiles_listens
,FREQUENCY as frequency
,IMPRESSIONS as impressions
,midpoints as midpoint_listens
,OFF_SPOTIFY_IMPRESSIONS as off_spotify_impressions
,PAID_LISTENS as paid_listens
,PAID_LISTENS_FREQUENCY as paid_listens_frequency
,PAID_LISTENS_REACH as paid_listens_reach
,REACH as reach
,SKIPS as skips
,SPEND as spend
,STARTS as start_listens
,THIRD_QUARTILES as third_quartiles_listen
,ac.account_name
,ads.ad_status
,ads.call_to_action_text
,ads.created_date as ad_created_date
,ad_sets.ad_set_category
,ad_sets.ad_set_name
,camps.campaign_name
,camps.created_date as campaign_create_date
,ad_sets.ad_set_created_date

FROM {{ref("daily_ad_history")}} add_hist
LEFT Join {{ref("spotify_accounts_view")}}  ac
  on ac.account_id = add_hist.ACCOUNT_ID
LEFT Join  {{ref("spotify_ads")}} ads
  on ads.ad_id = add_hist.ENTITY_ID
LEFT JOIN {{ref("spotify_ad_sets")}}  as ad_sets
  on ad_sets.ad_set_id = ads.ad_set_id
LEFT JOIN {{ref("spotify_campaigns")}} as camps
  on camps.campaign_id = ad_sets.campaign_id