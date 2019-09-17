##
from __future__ import print_function
import datetime
import json
import logging
import logging.config
import numpy as np
import os
import pandas as pd
import sys
import time
import yaml

from database.database_functions import (
    facebookconnect,
    bulk_upsert,
    find,
    extract_col,
    transform,
    get_request,
    request_to_database,
)
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.campaign import Campaign
from facebook_business.api import FacebookAdsApi
from database.models import (
    mySQL_connect,
#    AccountsTable,
#    CampaignsTable,
#    AdsInsightsTable,
#    AdsInsightsAgeGenderTable,
#    AdsInsightsRegionTable,
)
from sqlalchemy.orm import sessionmaker

#++++++++++++++++++++
# LOGGER
#++++++++++++++++++++

with open('database/config.yaml', 'r') as f:
    config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)

logger = logging.getLogger(__name__)

#+++++++++++++++++++++++++++++++++++++
# | FACEBOOK AUTHENTICATION |
#+++++++++++++++++++++++++++++++++++++

secrets = 'database/settings/fb_client_secrets.json'
try:
    facebookconnect(secrets_path=secrets)
    logger.info('Facebook authentication was a success')
except Exception as e:
    logging.exception('Failed to connect to Facebook')

#+++++++++++++++++++++++++++++++++++++
# ENGINE CONNECTION
#+++++++++++++++++++++++++++++++++++++

credentials = 'database/settings/db_secrets.json'
try:
    engine = mySQL_connect(credentials, port='3306', db='test_schema')
    logger.info('MySQL connection was a success')
except Exception as e:
    logging.exception('Failed to connect to MySQL')

#++++++++++++++++++++++++++++++++++++++++++
# | PARAMETERS FOR FACEBOOK API REQUESTS |
#++++++++++++++++++++++++++++++++++++++++++

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
    'date_preset': 'last_3d',
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
    'date_preset': 'last_3d',
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
    'date_preset': 'last_3d',
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

##
#+++++++++++++++++++++++++++++++++++++
# REQUESTS AND PUSHES
#+++++++++++++++++++++++++++++++++++++
##

def sleeper(seconds):
    for i in range(seconds, 0, -1):
        sys.stdout.write(str(i)+' ')
        sys.stdout.flush()
        time.sleep(1)

accounts_list = ['act_339963273316912']

for account in accounts_list:
    # ACCOUNTS TABLE
    account_request = get_request(account_id=account,
                                  table='accounts',
                                  params=account_params,
                                  fields=account_fields
                                  )

    request_to_database(request=account_request,
                        table='accounts',
                        engine=engine
                        )
    # CAMPAIGNS TABLE
    campaign_request = get_request(account_id=account,
                                   table='campaigns',
                                   params=campaign_params,
                                   fields=campaign_fields
                                   )

    request_to_database(request=campaign_request,
                        table='campaigns',
                        engine=engine
                        )

    # ADS INSIGHTS TABLE
    ads_request = get_request(account_id=account,
                              table='ads_insights',
                              params=ads_params,
                              fields=ads_fields
                              )

    request_to_database(request=ads_request,
                        table='ads_insights',
                        engine=engine
                        )

    # WAIT 10 MINUTES
    sleeper(600)

    # AGE AND GENDER TABLE
    agegender_request = get_request(account_id=account,
                                    table='ads_insights_age_and_gender',
                                    params=agegender_params,
                                    fields=agegender_fields
                                    )

    request_to_database(request=agegender_request,
                        table='ads_insights_age_and_gender',
                        engine=engine
                        )

    # WAIT 10 MINUTES
    sleeper(600)

    # REGION TABLE
    region_request = get_request(account_id=account,
                             table='ads_insights_region',
                             params=region_params,
                             fields=region_fields
                             )

    request_to_database(request=region_request,
                        table='ads_insights_region',
                        engine=engine
                        )
