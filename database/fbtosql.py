##
from __future__ import print_function
import datetime
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.campaign import Campaign
from facebook_business.api import FacebookAdsApi
import json
import logging
import logging.config
from models import (
    mySQL_connect,
    AccountsTable,
    CampaignsTable,
    AdsInsightsTable,
    AdsInsightsAgeGenderTable,
    AdsInsightsRegionTable,
)
import os
import pandas as pd
import pymysql
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.orm import sessionmaker
import time
import yaml
##
#++++++++++++++++++++
# LOGGER
#++++++++++++++++++++
##
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)

logger = logging.getLogger(__name__)
##
#++++++++++++++++++++++++++++++++++++++
# FACEBOOK CLIENT AUTHENTICATION
#++++++++++++++++++++++++++++++++++++++
##
client_secrets_path = '/home/wayned/acquire/credentials/facebook_business/client_secrets.json'
try:
    with open(client_secrets_path) as authentication_file:
            authentication_result = json.load(authentication_file)
    # READ AUTHENTICATION JSON FILE AND EXTRACT
    my_app_id = authentication_result['my_app_id']
    my_app_secret = authentication_result['my_app_secret']
    my_access_token = authentication_result['my_access_token']
    # AUTHENTICATE FACEBOOK API CALL WITH APP/USER CREDENTIALS
    FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)
    logger.info('Facebook authentication was a success')
except Exception as e:
    logging.exception('Facebook failed to authenticate')
##
#++++++++++++++++++++++++++++++++++++++++
# FUNCTIONS
#++++++++++++++++++++++++++++++++++++++++

def find(lst, key, value):
    """
    lst: a list of dictionaries
    returns: index of dictionary in list with (key,value)
    """
    for i, dic in enumerate(lst):
        if dic[key] == value:
            return i
    # value not found
    return -1

def extract_col(row, value):
    """row: a row from a pandas df
    value: a type of action  -- eg: mobile app installs
    returns: data associated with key, value pair
    """
    if type(row) != list:
        return 0
    else:
        index = find(lst=row, key='action_type', value=value)
        # index will be -1 if function cannot find value
        if index == -1:
            return 0
        return row[index]['value']

def bulk_upsert(session, table, df, id_col):
    """Perform a bulk insert of the given list of mapping dictionaries.
    The bulk insert feature allows plain Python dictionaries to be used
    as the source of simple INSERT operations which can be more easily
    grouped together into higher performing “executemany” operations.
    Using dictionaries, there is no “history” or session state management
    features in use, reducing latency when inserting large numbers of
    simple rows.
    --------------------------------------------------------------------
    table: a mapped class (i.e. database table)
    df: dataframe to be converted to a list of dictionaries
    """
    # getattr allows passing string argument to table object
    update_query = session.query(getattr(table, id_col)).filter(
        getattr(table, id_col).in_(list(pd.to_numeric(df[id_col])))
    )
    # initialize list of ids of records to update
    duplicate_keys = []
    duplicate_keys = [str(getattr(k, id_col)) for k in update_query]
    if session.query(table).count() > 0:
        # ids in query are integers, ids in df is str
        duplicate_keys = [str(getattr(k, id_col)) for k in update_query]
        update_df = df.loc[df[id_col].isin(duplicate_keys), ]
        update_df = update_df.to_dict(orient="records")
        session.bulk_update_mappings(
            table,
            update_df
        )

    insert_df = df.loc[df[id_col].isin(duplicate_keys) == False, ]
    insert_df = insert_df.to_dict(orient="records")
    # insert any records that do not already exist in db
    session.bulk_insert_mappings(
        table,
        insert_df,
        render_nulls=True
    )
    session.commit()
##
#+++++++++++++++++++++++++++++++++++++
# ENGINE CONNECTION
#+++++++++++++++++++++++++++++++++++++
##
credentials_path = '/home/wayned/acquire/credentials/database/credentials.json'
engine = mySQL_connect(credentials_path, port='3306', db='test_schema')
##
#+++++++++++++++++++++++++++++++++++++++
# ACCOUNT TABLE SCHEMA
#+++++++++++++++++++++++++++++++++++++++
##

my_account = AdAccount('act_22612640') # KOHO AD ACCOUNT
params = {'level': 'account'}

fields= [AdAccount.Field.account_id,
         AdAccount.Field.account_status,
         AdAccount.Field.amount_spent,
         ]

# should find a cleaner way to do this next bit
account_cursor = my_account.api_get(params=params, fields=fields)
request = dict(account_cursor)
columns = ['id', 'account_id', 'account_status', 'amount_spent']
account_df = pd.DataFrame(account_cursor.values(), columns).transpose()
account_df = account_df.drop(columns=['id']) # redundant for db

# insert into mysql
Session = sessionmaker(bind=engine)
session = Session()
bulk_upsert(session, AccountsTable, account_df, id_col='account_id')
session.close()
##
#++++++++++++++++++++++++++++++++++++++++
# CAMPAIGN TABLE SCHEMA
#++++++++++++++++++++++++++++++++++++++++
##
params = {'level': 'campaign',}

fields = [Campaign.Field.id,
          Campaign.Field.name,
          Campaign.Field.account_id,
          Campaign.Field.effective_status,
          Campaign.Field.updated_time,
          Campaign.Field.daily_budget,
          ]

campaign_cursor = my_account.get_campaigns(fields=fields, params=params)
request = [campaign for campaign in campaign_cursor]
# create a pandas df
columns = ['id', 'name', 'account_id', 'effective_status',
           'updated_time', 'daily_budget']
campaign_df = pd.DataFrame(request, columns=columns)
campaign_df.rename(columns={'id': 'campaign_id',
                            'name': 'campaign_name'},
                   inplace=True)
#campaign_df['campaign_id'] = pd.to_numeric(campaign_df['campaign_id'])
#campaign_df['account_id'] =pd.to_numeric(campaign_df['account_id'])
# work-around for nan error
campaign_df = campaign_df.where(pd.notnull(campaign_df), None)

# insert pandas dataframe into mysql database
Session = sessionmaker(bind=engine)
session = Session()
bulk_upsert(session, CampaignsTable, campaign_df, id_col='campaign_id')
session.close()

##
#++++++++++++++++++++++++++++++++++++++++
# ASYNCHRONOUS JOB -- ADS INSIGHTS TABLE
#++++++++++++++++++++++++++++++++++++++++
##
today = datetime.date.today()
yesturday = today - datetime.timedelta(days=1)
yesturday = yesturday.strftime('%Y-%m-%d')
first_of_month = datetime.date.today()
first_of_month = first_of_month.replace(day=1)
first_of_month = first_of_month.strftime('%Y-%m-%d')

params = {
    'date_preset': 'last_30d',
#    'time_range': {'since':first_of_month,
#                   'until':yesturday},
    'time_increment': 1,
    'level': 'ad',
        }

fields = [AdsInsights.Field.ad_id,
          AdsInsights.Field.campaign_id,
          AdsInsights.Field.date_start,
          AdsInsights.Field.date_stop,
          AdsInsights.Field.ad_name,
          AdsInsights.Field.campaign_name,
          AdsInsights.Field.account_id,
          AdsInsights.Field.frequency,
          AdsInsights.Field.cpc,
          AdsInsights.Field.cpm,
          AdsInsights.Field.spend,
          AdsInsights.Field.impressions,
          AdsInsights.Field.ctr,
          AdsInsights.Field.actions,
          ]

# extract data into json -- cursor object
insights_cursor = my_account.get_insights_async(fields=fields, params=params)
insights_cursor.api_get()
while insights_cursor[AdReportRun.Field.async_status] != "Job Completed":
    time.sleep(1)
    insights_cursor.api_get()
time.sleep(1)
request = insights_cursor.get_result(params={"limit": 1000})
##
#+++++++++++++++++++++++++++++++++++++++++
# CLEANING
#+++++++++++++++++++++++++++++++++++++++++
##
# column names must match eg: AdsInsights.Field.spend = spend
columns = ['ad_id', 'campaign_id', 'date_start','date_stop', 'ad_name',
           'campaign_name', 'account_id', 'frequency', 'cpc', 'cpm',
           'spend', 'impressions', 'ctr', 'actions']

# iterable (insights_cursor) will not work in older version of pandas
ads_insights_df = pd.DataFrame(request, columns=columns)
# cannot pull deleted campaign information
# remove ads with campaign ids that do not exist in
# the campaign table
ads_insights_df = ads_insights_df.loc[ads_insights_df['campaign_id'].isin(campaign_df['campaign_id']), :]


##
#++++++++++++++++++++++++++++++
# EXTRACTING FROM CAMPAIGN NAME
#++++++++++++++++++++++++++++++
##
# BUCKET
ads_insights_df.loc[ads_insights_df['campaign_name'].str.contains("Testing"), 'bucket'] = "Testing"
ads_insights_df.loc[ads_insights_df['campaign_name'].str.contains("WeKnow"), 'bucket'] = "WeKnow"
ads_insights_df.loc[ads_insights_df['campaign_name'].str.contains("LowEfficiency"), 'bucket'] = "LowEfficiency"
ads_insights_df.loc[ads_insights_df.bucket.isna(), 'bucket'] = "Other"

# PROSPECTING VS RETARGETING
ads_insights_df.loc[ads_insights_df['campaign_name'].str.contains("_PR_"), 'pr_rt'] = "PR"
ads_insights_df.loc[ads_insights_df['campaign_name'].str.contains("_RT_"), 'pr_rt'] = "RT"
ads_insights_df['pr_rt'].fillna("PR+RT", inplace=True)

# MOBILE INSTALLS
ads_insights_df['mobile_app_installs'] = ads_insights_df['actions'].apply(extract_col, value='mobile_app_install')
ads_insights_df["mobile_app_installs"] = pd.to_numeric(ads_insights_df["mobile_app_installs"])

# REGISTRATIONS
ads_insights_df['registrations_completed'] = ads_insights_df['actions'].apply(extract_col, value='app_custom_event.fb_mobile_complete_registration')
ads_insights_df['registrations_completed'] = pd.to_numeric(ads_insights_df['registrations_completed'])

# LINK CLICKS
ads_insights_df['clicks'] = ads_insights_df['actions'].apply(extract_col, value='link_click')

# drop actions and video actions column
ads_insights_df = ads_insights_df.drop(columns=['actions'])

# work-around for nan error -> convert to None type
ads_insights_df = ads_insights_df.where(pd.notnull(ads_insights_df), None)

# insert into Ads Insights table
Session = sessionmaker(bind=engine)
session = Session()
bulk_upsert(session, AdsInsightsTable, ads_insights_df, id_col='ad_id')
session.close()

#+++++++++++++++++++++++++++++++++++++++++++++++++
# ADS INSIGHTS AGE AND GENDER TABLE
#+++++++++++++++++++++++++++++++++++++++++++++++++
##
params = {
    'date_preset': 'last_30d',
#    'time_range': {'since':first_of_month,
#                   'until':yesturday},
    'time_increment': 1,
    'level': 'ad',
    'breakdowns': ['age', 'gender'],
        }

fields = [AdsInsights.Field.ad_id,
          AdsInsights.Field.account_id,
          AdsInsights.Field.campaign_id,
          AdsInsights.Field.date_start,
          # age
          # gender
          AdsInsights.Field.frequency,
          AdsInsights.Field.cpc,
          AdsInsights.Field.cpm,
          AdsInsights.Field.spend,
          AdsInsights.Field.impressions,
          AdsInsights.Field.ctr,
          AdsInsights.Field.actions,
          ]

# extract data into json -- cursor object
insights_cursor = my_account.get_insights_async(fields=fields, params=params)
insights_cursor.api_get()
while insights_cursor[AdReportRun.Field.async_status] != "Job Completed":
    time.sleep(1)
    insights_cursor.api_get()
time.sleep(1)
request = insights_cursor.get_result(params={"limit": 1000})
##
#+++++++++++++++++++++++++++++++++++++++++
# CLEANING
#+++++++++++++++++++++++++++++++++++++++++
##
# column names must match eg: AdsInsights.Field.spend = spend
columns = ['ad_id', 'account_id', 'campaign_id',
           'date_start', 'age', 'gender',
           'frequency', 'cpc', 'cpm',
           'spend', 'impressions', 'ctr', 'actions']

# iterable (insights_cursor) will not work in older version of pandas
ads_insights_age_gender_df = pd.DataFrame(request, columns=columns)
##
# cannot pull deleted campaign information
# remove ads with campaign ids that do not exist in
# the campaign table
ads_insights_age_gender_df = ads_insights_age_gender_df.loc[ads_insights_age_gender_df['campaign_id'].isin(campaign_df['campaign_id']), :]

# MOBILE INSTALLS
ads_insights_age_gender_df['mobile_app_installs'] = ads_insights_age_gender_df['actions'].apply(extract_col, value='mobile_app_install')
ads_insights_age_gender_df["mobile_app_installs"] = pd.to_numeric(ads_insights_age_gender_df["mobile_app_installs"])

# REGISTRATIONS
ads_insights_age_gender_df['registrations_completed'] = ads_insights_age_gender_df['actions'].apply(extract_col, value='app_custom_event.fb_mobile_complete_registration')
ads_insights_age_gender_df['registrations_completed'] = pd.to_numeric(ads_insights_age_gender_df['registrations_completed'])

# LINK CLICKS
ads_insights_age_gender_df['clicks'] = ads_insights_age_gender_df['actions'].apply(extract_col, value='link_click')

# drop actions and video actions column
ads_insights_age_gender_df = ads_insights_age_gender_df.drop(columns=['actions'])

# work-around for nan error -> convert to None type
ads_insights_age_gender_df = ads_insights_age_gender_df.where(pd.notnull(ads_insights_age_gender_df), None)
##
# insert into Ads Insights table
Session = sessionmaker(bind=engine)
session = Session()
bulk_upsert(session, AdsInsightsAgeGenderTable, ads_insights_age_gender_df, id_col='ad_id')
session.close()
##
#+++++++++++++++++++++++++++++++++++++++++++++++++
# ADS INSIGHTS REGION TABLE 
#+++++++++++++++++++++++++++++++++++++++++++++++++
##
params = {
    'date_preset': 'last_30d',
#    'time_range': {'since':first_of_month,
#                   'until':yesturday},
    'time_increment': 1,
    'level': 'ad',
    'breakdowns': ['region'],
        }

fields = [AdsInsights.Field.ad_id,
          AdsInsights.Field.account_id,
          AdsInsights.Field.campaign_id,
          AdsInsights.Field.date_start,
          # region
          AdsInsights.Field.frequency,
          AdsInsights.Field.cpc,
          AdsInsights.Field.cpm,
          AdsInsights.Field.spend,
          AdsInsights.Field.impressions,
          AdsInsights.Field.ctr,
          AdsInsights.Field.actions,
          ]

# extract data into json -- cursor object
insights_cursor = my_account.get_insights_async(fields=fields, params=params)
insights_cursor.api_get()
while insights_cursor[AdReportRun.Field.async_status] != "Job Completed":
    time.sleep(1)
    insights_cursor.api_get()
time.sleep(1)
request = insights_cursor.get_result(params={"limit": 1000})
##
#+++++++++++++++++++++++++++++++++++++++++
# CLEANING
#+++++++++++++++++++++++++++++++++++++++++
##
# column names must match eg: AdsInsights.Field.spend = spend
columns = ['ad_id', 'account_id', 'campaign_id',
           'date_start', 'region',
           'frequency', 'cpc', 'cpm',
           'spend', 'impressions', 'ctr', 'actions']

# iterable (insights_cursor) will not work in older version of pandas
ads_insights_region_df = pd.DataFrame(request, columns=columns)
##
# cannot pull deleted campaign information
# remove ads with campaign ids that do not exist in
# the campaign table
ads_insights_region_df = ads_insights_region_df.loc[ads_insights_region_df['campaign_id'].isin(campaign_df['campaign_id']), :]
#ads_insights_region_df = ads_insights_region_df.loc[ads_insights_region_df['ad_id'].isin(ads_insights_df['ad_id']), :]
# MOBILE INSTALLS
ads_insights_region_df['mobile_app_installs'] = ads_insights_region_df['actions'].apply(extract_col, value='mobile_app_install')
ads_insights_region_df["mobile_app_installs"] = pd.to_numeric(ads_insights_region_df["mobile_app_installs"])

# REGISTRATIONS
ads_insights_region_df['registrations_completed'] = ads_insights_region_df['actions'].apply(extract_col, value='app_custom_event.fb_mobile_complete_registration')
ads_insights_region_df['registrations_completed'] = pd.to_numeric(ads_insights_region_df['registrations_completed'])

# LINK CLICKS
ads_insights_region_df['clicks'] = ads_insights_region_df['actions'].apply(extract_col, value='link_click')

# drop actions and video actions column
ads_insights_region_df = ads_insights_region_df.drop(columns=['actions'])

# work-around for nan error -> convert to None type
ads_insights_region_df = ads_insights_region_df.where(pd.notnull(ads_insights_region_df), None)
##
# insert into Ads Insights table
Session = sessionmaker(bind=engine)
session = Session()
bulk_upsert(session, AdsInsightsRegionTable, ads_insights_region_df, id_col='ad_id')
session.close()
