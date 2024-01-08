import requests
from dagster import asset, AssetExecutionContext, MetadataValue
# from ..partitions import monthly_partition
import pandas as pd
import os
from datetime import datetime, timedelta
import time
from dagster_gcp import BigQueryResource


def get_start_end_dates(input_date):
    # Convert input date to a datetime object
    input_datetime = datetime.fromisoformat(input_date)

    # Get today's date
    today = datetime.today()

    # Initialize the list of start and end dates
    start_and_end_dates = []

    while input_datetime < today:
        # Get the first day of the month
        start_of_month = input_datetime.replace(day=1)

        # Calculate the last day of the current month
        if start_of_month.month == 12:
            next_month = start_of_month.replace(year=start_of_month.year + 1, month=1)
        else:
            next_month = start_of_month.replace(month=start_of_month.month + 1)
        end_of_month = next_month - timedelta(days=1)

        # Format the results as strings in ISO 8601 format
        start_of_month_iso = start_of_month.isoformat() + "Z"
        end_of_month_iso = end_of_month.isoformat() + "Z"

        # Add the start and end dates to the list
        start_and_end_dates.append((start_of_month_iso, end_of_month_iso))

        # Move to the next month
        input_datetime = next_month

    return start_and_end_dates

def get_ads_stats(token, ad_id, start_date, end_date, ad_account_id):
    headers = {
        "Authorization": "Bearer " + token
    }
    report_endpoint = f"https://api-partner.spotify.com/ads/v2/ad_accounts/{ad_account_id}/aggregate_reports?entity_type=AD&report_start={start_date}&report_end={end_date}&granularity=DAY&fields=CLICKS,COMPLETES,CTR,E_CPM,E_CPCL,FIRST_QUARTILES,MIDPOINTS,THIRD_QUARTILES,FREQUENCY,IMPRESSIONS,OFF_SPOTIFY_IMPRESSIONS,PAID_LISTENS,REACH,SPEND,STARTS,SKIPS,PAID_LISTENS_REACH,PAID_LISTENS_FREQUENCY&entity_ids={ad_id}&entity_ids_type=AD"

    stats = requests.get(report_endpoint, headers=headers)
    while stats.status_code == 429:
        time.sleep(10)
        stats = requests.get(report_endpoint, headers=headers)
    
    #print(stats.status_code)
    if stats.status_code != 200:
        print(stats.text)
    if 'rows' not in stats.json():
        print(ad_id, stats.json())
        return None
    stats_df = pd.json_normalize(stats.json(), record_path=['rows'])
    #print(len(stats_df))
    if len(stats_df) == 0:
        return None
    # Explode the 'stats' column
    exploded_df = stats_df.explode('stats')

    # Normalize the exploded 'stats' column
    flattened_stats = pd.json_normalize(exploded_df['stats'])

    # Reset the index of the exploded DataFrame
    exploded_df = exploded_df.reset_index(drop=True)

    # Reset the index of the flattened DataFrame
    flattened_stats = flattened_stats.reset_index(drop=True)

    # Join the flattened stats back to the exploded DataFrame
    final_df = pd.concat([exploded_df, flattened_stats], axis=1)
    final_df.reset_index(drop=True, inplace=True)
    index_columns = [col for col in final_df.columns if col not in ['field_type', 'field_value', 'stats', 'index']]
    index_columns = ['entity_id', 'start_time', 'end_time', 'entity_name']
    pivot_df = final_df.pivot(index=index_columns, columns='field_type', values='field_value')
    pivot_df = pivot_df.reset_index()
    pivot_df['account_id'] = ad_account_id
    pivot_df.columns = pivot_df.columns.str.replace('.', '_')
    return pivot_df


def get_start_and_end_of_month(input_date):
    # Convert input date to a datetime object
    input_datetime = datetime.fromisoformat(input_date)

    # Get the first day of the month
    start_of_month = input_datetime.replace(day=1)

    # Calculate the last day of the current month
    next_month = start_of_month.replace(month=start_of_month.month + 1)
    end_of_month = next_month - timedelta(days=1)

    # Format the results as strings in ISO 8601 format
    start_of_month_iso = start_of_month.isoformat() + "Z"
    end_of_month_iso = end_of_month.isoformat() + "Z"

    return start_of_month_iso, end_of_month_iso

def get_new_token()-> str:
    refresh_token = os.getenv('SPOTIFY_REFRESH_TOKEN')
    url = "https://accounts.spotify.com/api/token"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": "4717de5d14ca47a288cd0f63ab195144",
        "client_secret": "767f6f11016c492980e6372b3dbeab6d",
    }
    response = requests.post(url, data=data)
    access_token = response.json()['access_token']
    os.environ['SPOTIFY_BEARER_TOKEN'] = access_token   
    return access_token


@asset(group_name="spotify_ingestion",
       compute_kind="python",)
def accounts(context: AssetExecutionContext) -> pd.DataFrame: 
    """
    gets all spotify accounts for the user
    """
    token = get_new_token()
    # token = os.getenv('SPOTIFY_BEARER_TOKEN')
    headers = {
        "Authorization": "Bearer " + token
    }
    accounts_url = "https://api-partner.spotify.com/ads/v2/ad_accounts"

    response = requests.get(accounts_url, headers=headers)

    if response.status_code == 200:
        accounts = response.json()['ad_accounts']
        accounts_df = pd.json_normalize(accounts)
        # output_path = os.path.join("data", "staging" , "spotify_accounts.csv")
        # accounts_df.to_csv(output_path, index=False)

        context.add_output_metadata(
        {
            "num_records": len(accounts_df),
            "preview": MetadataValue.md(accounts_df.head().to_markdown()),
        }
        )
        return accounts_df
    else:
        print("Error retrieving Spotify ad accounts: " + response.text)


@asset(group_name="spotify_ingestion",
       compute_kind="python",)
def campaigns(context: AssetExecutionContext, accounts: pd.DataFrame)-> pd.DataFrame:
    """
    gets all campaigns for each account
    """
    token = get_new_token()
    campaign_df = pd.DataFrame()  # Create an empty dataframe to store the results

    for account_id in accounts['id']:
        # Query for campaign results using the account_id
        campaign_results_url = f"https://api-partner.spotify.com/ads/v2/ad_accounts/{account_id}/campaigns"
        headers = {
            "Authorization": "Bearer " + token
        }
        response = requests.get(campaign_results_url, headers=headers)

        if response.status_code == 200:
            campaign_results = response.json()["campaigns"]
            # Transform the results to a pandas dataframe
            campaign_results_df = pd.json_normalize(campaign_results)
            # Add the account_id as a column
            campaign_results_df["account_id"] = account_id
            campaign_df = pd.concat([campaign_df, campaign_results_df], ignore_index=True)
        else:
            print(f"Error retrieving campaign results for account {account_id}: " + response.text)

    context.add_output_metadata(
    {
        "num_records": len(campaign_df),
        "preview": MetadataValue.md(campaign_df.head().to_markdown()),
    }
    )

    return campaign_df


@asset(group_name="spotify_ingestion",
       compute_kind="python",)
def ad_sets(context: AssetExecutionContext, accounts: pd.DataFrame) -> pd.DataFrame:
    """
    gets all ads for each account
    """
    token = get_new_token()
    headers = {
        "Authorization": "Bearer " + token
    }
    
    # need to iterate through the accounts
    results_df = pd.DataFrame()
    for account_id in accounts['id']:
        ad_sets_endpoint = f"https://api-partner.spotify.com/ads/v2/ad_accounts/{account_id}/ad_sets"
        response = requests.get(ad_sets_endpoint, headers=headers)
        print(response.status_code)
        # print(response.json())
        total_records = response.json()["paging"]["total_results"]
        offset = response.json()["paging"]["page_size"]
        ads_df = pd.json_normalize(response.json()["ad_sets"])

        records_pulled = offset
        while total_records > records_pulled:
        # paginate through the results
            ads_endpoint = f"https://api-partner.spotify.com/ads/v2/ad_accounts/{account_id}/ads?offset={offset}"
            response = requests.get(ads_endpoint, headers=headers)
            print(response.status_code)
            records_pulled = records_pulled + response.json()["paging"]["page_size"]

            ads_df = pd.concat([ads_df, pd.json_normalize(response.json()["ad_sets"])], ignore_index=True)
            ads_df['account_id'] = account_id
        results_df = pd.concat([results_df, ads_df], ignore_index=True)
        print('Length of Dataframe', len(ads_df))
        print('Total Records(API):', total_records)
    results_df.columns = results_df.columns.str.replace('.', '_')
    
    context.add_output_metadata(
    {
        "num_records": len(results_df),
        "preview": MetadataValue.md(results_df.head().to_markdown()),
    }
    )
    return results_df

@asset(group_name="spotify_ingestion",
       compute_kind="python",)
def ads(context: AssetExecutionContext, accounts: pd.DataFrame) -> pd.DataFrame:
    """
    gets all ads for each account
    """
    token = get_new_token()
    headers = {
        "Authorization": "Bearer " + token
    }
    
    # need to iterate through the accounts
    results_df = pd.DataFrame()
    for account_id in accounts['id']:
        ads_endpoint = f"https://api-partner.spotify.com/ads/v2/ad_accounts/{account_id}/ads"
        response = requests.get(ads_endpoint, headers=headers)
        print(response.status_code)
        # print(response.json())
        total_records = response.json()["paging"]["total_results"]
        offset = response.json()["paging"]["page_size"]
        ads_df = pd.json_normalize(response.json()["ads"])

        records_pulled = offset
        while total_records > records_pulled:
        # paginate through the results
            ads_endpoint = f"https://api-partner.spotify.com/ads/v2/ad_accounts/{account_id}/ads?offset={offset}"
            response = requests.get(ads_endpoint, headers=headers)
            print(response.status_code)
            records_pulled = records_pulled + response.json()["paging"]["page_size"]

            ads_df = pd.concat([ads_df, pd.json_normalize(response.json()["ads"])], ignore_index=True)
        
        ads_df['account_id'] = account_id
        results_df = pd.concat([results_df, ads_df], ignore_index=True)
    results_df.columns = results_df.columns.str.replace('.', '_')
    
    context.add_output_metadata(
    {
        "num_records": len(results_df),
        "preview": MetadataValue.md(results_df.head().to_markdown()),
    }
    )
    return results_df



@asset(group_name="spotify_ingestion",
       compute_kind="python",)
def initial_history_ad_stats_daily(context: AssetExecutionContext, ads: pd.DataFrame) -> pd.DataFrame:
    """
    gets all ad stats for each ad and appends them to a dataframe 
    """
    token = get_new_token()
    results = pd.DataFrame()
    for index, row in ads.iterrows():
        dates = get_start_end_dates(row['created_at'][:10])
        for date in dates:
            ad_stats = get_ads_stats(token, row['id'], date[0], date[1], row['account_id'])
            results = pd.concat([results, ad_stats], ignore_index=True)
    results.columns = results.columns.str.replace('.', '_')

    context.add_output_metadata(
    {
        "num_records": len(results),
        "preview": MetadataValue.md(results.head().to_markdown()),
    }
    )    
    return results


@asset(group_name="spotify_ingestion",
       compute_kind="python",
       deps=[accounts],)
def get_last_update_date(context: AssetExecutionContext, bigquery: BigQueryResource) -> pd.DataFrame:
    """
    gets the last date that the bigquery table was updated
    """
    with bigquery.get_client() as client:
        query_job = client.query(
            'SELECT cast(left(end_time,10) as date) as last_update FROM spotify_ads.full_history_ad_stats_daily ORDER BY cast(left(end_time,10) as date) DESC LIMIT 1',
        )
        df = query_job.result().to_dataframe()

    context.add_output_metadata(
    {
        "preview": MetadataValue.md(df.to_markdown()),
    }
    )   
    return df


@asset(group_name="spotify_ingestion",
       compute_kind="python",)
def incremental_refresh_ad_stats_daily(context: AssetExecutionContext, get_last_update_date: pd.DataFrame
                                  , ads : pd.DataFrame) -> pd.DataFrame:
    """
    gets the incremental refresh and loads it to a bigquery staging table
    """
    token = get_new_token()
    results = pd.DataFrame()
    # Get the first date of the previous month at time zero
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).isoformat()+'Z'
    first_of_this_month = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    first_of_previous_month = (first_of_this_month - timedelta(days=1)).replace(day=1).isoformat()+'Z'

    # get todays date and the start date of the month prior
    active_ads = ads[ads['delivery']=='ON']
    for index, row in active_ads.iterrows():
        ad_stats = get_ads_stats(token, row['id'], first_of_previous_month, today, row['account_id'])
        results = pd.concat([results, ad_stats], ignore_index=True)
    results.columns = results.columns.str.replace('.', '_')
    context.add_output_metadata(
    {
        "num_records": len(results),
        "preview": MetadataValue.md(results.head().to_markdown()),
    }
    ) 
    return results


