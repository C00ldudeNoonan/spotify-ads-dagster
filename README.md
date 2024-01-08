# Spotify_ads_Dagster
 Repo for ingesting spotify ads data into bigquery using Dagster and dbt. This repo will should be helpful for anyone that needs to ingest Spotify ads data for reporting into a data warehouse. 

# Dagster Assets
![Dagster Asset Graph](./Spotify-ads-full-view.png)


I have Dagster assets that materialize the main entities that you can pull from the Spotify Ads API (Accounts, Campaigns, Ad Sets, Ads), The daily performance history going back to the beginning of the account, and an incremental refresh of daily ad sets that goes back to the start of the previous month to today. I use pandas dataframes as the main asset input and output and configured the BigQuery i/o manager for this. 

# dbt models

In dbt I created basic models for all the entities mentioned above as well as a daily ad history view and finally an analysis table ready for a BI tool. 

# Environment Variables

 My .env file looked like this:
 SPOTIFY_REFRESH_TOKEN=
 BIGQUERY_PROJECT_ID=
 BIGQUERY_SERVICE_ACCOUNT_CREDENTIALS=
 BIGQUERY_DATASET_ID=
 PRIVATE_KEY_ID=
 PRIVATE_KEY=
 CLIENT_EMAIL=
 CLIENT_ID=
 AUTH_URI=
 AUTH_PROVIDER_X509_CERT_URL=
 CLIENT_X509_CERT_URL=
 DAGSTER_DBT_PARSE_PROJECT_ON_LOAD=1