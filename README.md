# Linkedin-Interface

The LinkedIn Data Interface module retrieves advertising campaign performance metrics (impressions, clicks, spend) for all campaigns in a specified campaign group, then writes the consolidated data to a Spark table (pocn_data.silver.LinkedIn_Campaign_Insights) for downstream analytics.

[LinkedIn Ads API] 
      ↓ (REST calls, pagination)
[Python Script] 
     ├─ get_campaign_ids_and_names()
     ├─ get_campaign_insights()
     └─ pandas.DataFrame
      ↓ (Spark conversion)
[PySpark Job] → writes to Hive metastore table
