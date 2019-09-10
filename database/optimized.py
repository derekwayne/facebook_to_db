##
from __future__ import print_function
import datetime
import json
import logging
import logging.config
import numpy as np
import os
import pandas as pd
import time
import yaml

from database_functions import (
    facebookconnect,
    bulk_upsert,
    find,
    extract_col,
    get_request,
)
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.campaign import Campaign
from facebook_business.api import FacebookAdsApi
from models import (
    mySQL_connect,
    AccountsTable,
    CampaignsTable,
    AdsInsightsTable,
    AdsInsightsAgeGenderTable,
    AdsInsightsRegionTable,
)
from sqlalchemy.orm import sessionmaker
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
#+++++++++++++++++++++++++++++++++++++
# | FACEBOOK AUTHENTICATION |
#+++++++++++++++++++++++++++++++++++++
##
secrets = '/home/wayned/acquire/credentials/facebook_business/client_secrets.json'
try:
    facebookconnect(secrets_path=secrets)
    logger.info('Facebook authentication was a success')
except Exception as e:
    logging.exception('Failed to connect to Facebook')
##
#+++++++++++++++++++++++++++++++++++++
# ENGINE CONNECTION
#+++++++++++++++++++++++++++++++++++++
##
credentials = '/home/wayned/acquire/credentials/database/credentials.json'
try:
    engine = mySQL_connect(credentials, port='3306', db='test_schema')
    logger.info('MySQL connection was a success')
except Exception as e:
    logging.exception('Failed to connect to MySQL')
##
#++++++++++++++++++++++++++++++++++++++++++
# | PARAMETERS FOR FACEBOOK API REQUESTS |
#++++++++++++++++++++++++++++++++++++++++++
##
# ACCOUNT
account_params = {'level': 'account'}
account_fields = [AdAccount.Field.account_id,
                  AdAccount.Field.account_status,
                  AdAccount.Field.amount_spent,
                  ]
# CAMPAIGN
campaign_params = {'level': 'campaign',}
campaign_fields = [Campaign.Field.id,
                   Campaign.Field.name,
                   Campaign.Field.account_id,
                   Campaign.Field.effective_status,
                   Campaign.Field.updated_time,
                   Campaign.Field.daily_budget,
                   ]
# ADS
ads_params = {
    'date_preset': 'last_30d',
    'time_increment': 1,
    'level': 'ad',
}
ads_fields = [AdsInsights.Field.ad_id,
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
# ADS - AGE AND GENDER
agegender_params = {
    'date_preset': 'last_30d',
    'time_increment': 1,
    'level': 'ad',
    'breakdowns': ['age', 'gender'],
}

agegender_fields = [AdsInsights.Field.ad_id,
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
# ADS - REGION
region_params = {
    'date_preset': 'last_30d',
    'time_increment': 1,
    'level': 'ad',
    'breakdowns': ['region'],
}

region_fields = [AdsInsights.Field.ad_id,
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

#++++++++++++++++++++++++++++++++++++++++
# | UPSERTING REQUEST DATA TO DATABASE
#++++++++++++++++++++++++++++++++++++++++

def request_to_database(request, table, engine):
    """Take a facebook api request, load data into a
    pandas dataframe, perform column operations for
    specified table and upsert into mysql database.
    table: database table name as type: str
    engine: database engine
    """
    # read json file containing datatype info
    with open('columns/' + table + '.json') as f:
        dtypes = json.load(f)
    columns = list(dtypes.keys()) # create lost of colnames

    """[bug report] must treat accounts df creation separately for now
    values of columns are scalars and not lists (since
    there is only one observation). This requires passing
    the index argument to DataFrame below.
    """
    # create temporary dataframe objects in order to
    # use pandas library
    if table == 'accounts':
         df = pd.DataFrame(request,
                           columns = columns,
                           index=[0]
                          ).astype(dtype=dtypes)
         df.to_csv('data/accounts.csv') # store df for manual check
         logger.info('accounts dataframe created')
    else:
        df = pd.DataFrame(request,
                          columns = columns
                          ).astype(dtype=dtypes)

        logger.info('%s dataframe created', table)
    # build session with MySQL
    Session = sessionmaker(bind=engine)
    session = Session()
    # dataframes inserted or updated into database
    if table == 'accounts':
        bulk_upsert(session, AccountsTable,
                     df, id_col='account_id')
        logger.info('Accounts table has been synced to database')

    if table == 'campaigns':
        # must rename these columns due to Field class attributes
        # from parent Campaign (see facebook-business)
        df.rename(columns={'id': 'campaign_id',
                           'name': 'campaign_name'},
                  inplace=True)
        # nans throw errors so swap with None
        df = df.where(pd.notnull(df), None)

        bulk_upsert(session, CampaignsTable,
                   df, id_col='campaign_id')
        logger.info('Campaigns table has been synced to database')

    if table == 'ads_insights':
        print(table)
        # campaign id may refer to a deleted campaign which
        # is not contained in the Campaigns table of the database
        # we keep only those ids which are;
        campaign_ids = session.query(CampaignsTable.campaign_id)
        campaign_ids = [i for i, in campaign_ids]
        print(campaign_ids)
        df = df.loc[df['campaign_id'].isin(campaign_ids), :]

    df.to_csv('data/' + table + '.csv')
    session.close()

account_request = get_request(account_id='act_22612640',
                              table='accounts',
                              params=account_params,
                              fields=account_fields
                              )

request_to_database(request=account_request,
                    table='accounts',
                    engine=engine
                    )

campaign_request = get_request(account_id='act_22612640',
                               table='campaigns',
                               params=campaign_params,
                               fields=campaign_fields
                               )

request_to_database(request=campaign_request,
                    table='campaigns',
                    engine=engine
                    )
##
ads_request = get_request(account_id='act_22612640',
                          table='ads_insights',
                          params=ads_params,
                          fields=ads_fields
                          )
##
request_to_database(request=ads_request,
                    table='ads_insights',
                    engine=engine
                    )

##
