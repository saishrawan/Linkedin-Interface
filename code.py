import requests
import json
import pandas as pd
import time

# Set the headers for API requests
access_token = 'XXXX'

headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json',
    'LinkedIn-Version': '202505'
}

# Advertiser Account ID and Campaign Group ID for POCN_Campaigns
ad_account_id = '503472428'
campaign_group_id = '609549313'
page_size = 1000  # Adjust as needed, max allowed is 1000

# Step 1: Fetch all campaign IDs and names within the campaign group with pagination
def get_campaign_ids_and_names(ad_account_id, campaign_group_id):
    campaigns = []
    next_page_token = None

    while True:
        campaign_group_url = (
            f'https://api.linkedin.com/rest/adAccounts/{ad_account_id}/adCampaigns?q=search&'
            f'search.campaignGroup.values[0]=urn:li:sponsoredCampaignGroup:{campaign_group_id}&'
            f'pageSize={page_size}'
        )
        if next_page_token:
            campaign_group_url += f'&pageToken={next_page_token}'

        response = requests.get(campaign_group_url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            campaigns.extend([{'id': campaign['id'], 'name': campaign['name']} for campaign in data['elements']])
            next_page_token = data.get('metadata', {}).get('nextPageToken')
            if not next_page_token:
                break
        else:
            print(f"Error fetching campaign IDs: {response.status_code}")
            print(response.text)
            break

    return campaigns

campaigns = get_campaign_ids_and_names(ad_account_id, campaign_group_id)
print("Total Campaigns:", len(campaigns))

# Step 2: Fetch campaign insights for each campaign ID with retry mechanism
def get_campaign_insights(campaign_id, start_date, end_date, retries=3, delay=2):
    campaign_insights_url = (
        f'https://api.linkedin.com/rest/adAnalytics?q=analytics&pivot=CAMPAIGN&timeGranularity=ALL&'
        f'campaigns=urn:li:sponsoredCampaign:{campaign_id}&'
        f'dateRange.start.year={start_date[:4]}&dateRange.start.month={start_date[5:7]}&dateRange.start.day={start_date[8:]}&'
        f'dateRange.end.year={end_date[:4]}&dateRange.end.month={end_date[5:7]}&dateRange.end.day={end_date[8:]}&'
        f'fields=impressions,clicks,costInUsd'
    )
    
    for attempt in range(retries):
        response = requests.get(campaign_insights_url, headers=headers)
        if response.status_code == 200:
            return response.json()
        elif response.status_code >= 500:
            print(f"Server error for campaign {campaign_id}, attempt {attempt + 1}/{retries}. Retrying in {delay} seconds...")
            time.sleep(delay)
        else:
            print(f"Error fetching insights for campaign {campaign_id}: {response.status_code}")
            print(response.text)
            return None
    print(f"Failed to fetch insights for campaign {campaign_id} after {retries} attempts.")
    return None

# Fetch performance data for each campaign
start_date = begin_date  # Adjust as needed
end_date = ending_date    # Adjust as needed

all_campaign_insights = []
no_data_campaigns = []
failed_campaigns = []

for campaign in campaigns:
    campaign_id = campaign['id']
    campaign_name = campaign['name']
    insights = get_campaign_insights(campaign_id, start_date, end_date)
    if insights and 'elements' in insights:
        if insights['elements']:
            for element in insights['elements']:
                all_campaign_insights.append({
                    'campaign_id': campaign_id,
                    'campaign_name': campaign_name,
                    'campaign_group': 'POCN_Campaigns',
                    'spent': element.get('costInUsd', 'N/A'),
                    'impressions': element.get('impressions', 'N/A'),
                    'clicks': element.get('clicks', 'N/A'),
                    'date': start_date  
                })
        else:
            no_data_campaigns.append(campaign_id)
    else:
        failed_campaigns.append(campaign_id)

print("Total Campaigns with Insights:", len(all_campaign_insights))
print("Campaigns with No Data:", no_data_campaigns)
print("Failed Campaigns:", failed_campaigns)

# Convert to DataFrame
df = pd.DataFrame(all_campaign_insights)
spark_df = spark.createDataFrame(df)

table_name = "LinkedIn_Campaign_Insights"
database_name = "pocn_data.silver"

# Save the DataFrame as a table in the specified database
spark_df.write.mode("append").saveAsTable(f"{database_name}.{table_name}")

# Print the DataFrame
print("All Campaign Performance Data:")
print(df.head())
