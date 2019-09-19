##
from __future__ import print_function
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
    batch_dates,
)
from datetime import datetime
from datetime import timedelta
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebook_business.api import FacebookAdsApi
from database.models import (
    mySQL_connect,
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
    engine = mySQL_connect(credentials, port='3306', db='acquire')
    logger.info('MySQL connection was a success')
except Exception as e:
    logging.exception('Failed to connect to MySQL')

#++++++++++++++++++++++++++++++++++++++++++
# | PARAMETERS FOR FACEBOOK API REQUESTS |
#++++++++++++++++++++++++++++++++++++++++++

# ACCOUNT
account_params = {'level': 'account'}
account_fields = [AdAccount.Field.account_id,
                  AdAccount.Field.name,
                  AdAccount.Field.account_status,
                  AdAccount.Field.currency,
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
# AD SETS
adset_params = {'level': 'adset'}
adset_fields = [AdSet.Field.id,
          AdSet.Field.name,
          AdSet.Field.account_id,
          AdSet.Field.campaign_id,
          AdSet.Field.created_time,
          AdSet.Field.daily_budget,
          AdSet.Field.status,
          AdSet.Field.optimization_goal,
          AdSet.Field.updated_time
          ]

# ADS
ads_params = {
    'time_increment': 1,
    'action_attribution_windows': ['1d_view', '7d_view',
                                   '28d_view', '1d_click',
                                   '7d_click', '28d_click'],
    'level': 'ad',
        }
ads_fields = [AdsInsights.Field.ad_id,
              AdsInsights.Field.account_id,
              AdsInsights.Field.campaign_id,
              AdsInsights.Field.adset_id,
              AdsInsights.Field.date_start,
              AdsInsights.Field.account_name,
              AdsInsights.Field.campaign_name,
              AdsInsights.Field.adset_name,
              AdsInsights.Field.ad_name,
              AdsInsights.Field.spend,
              AdsInsights.Field.account_currency,
              AdsInsights.Field.frequency,
              AdsInsights.Field.reach,
              AdsInsights.Field.impressions,
              AdsInsights.Field.actions,
              AdsInsights.Field.action_values,
              ]
# ADS - AGE AND GENDER
agegender_params = {
    'time_increment': 1,
    'level': 'ad',
    'action_attribution_windows': ['1d_view', '7d_view',
                                   '28d_view', '1d_click',
                                   '7d_click', '28d_click'],
    'breakdowns': ['age', 'gender'],
}

agegender_fields = [AdsInsights.Field.ad_id,
                    AdsInsights.Field.account_id,
                    AdsInsights.Field.campaign_id,
                    AdsInsights.Field.adset_id,
                    AdsInsights.Field.date_start,
                    AdsInsights.Field.account_name,
                    AdsInsights.Field.campaign_name,
                    AdsInsights.Field.adset_name,
                    AdsInsights.Field.ad_name,
                    AdsInsights.Field.spend,
                    AdsInsights.Field.account_currency,
                    AdsInsights.Field.frequency,
                    AdsInsights.Field.reach,
                    AdsInsights.Field.impressions,
                    AdsInsights.Field.actions,
                    AdsInsights.Field.action_values,
                    ]

# ADS - REGION
region_params = {
    'time_increment': 1,
    'level': 'ad',
    'action_attribution_windows': ['1d_view', '7d_view',
                                   '28d_view', '1d_click',
                                   '7d_click', '28d_click'],
    'breakdowns': ['region'],
}

region_fields = [AdsInsights.Field.ad_id,
                 AdsInsights.Field.account_id,
                 AdsInsights.Field.campaign_id,
                 AdsInsights.Field.adset_id,
                 AdsInsights.Field.date_start,
                 AdsInsights.Field.account_name,
                 AdsInsights.Field.campaign_name,
                 AdsInsights.Field.adset_name,
                 AdsInsights.Field.ad_name,
                 AdsInsights.Field.spend,
                 AdsInsights.Field.account_currency,
                 AdsInsights.Field.frequency,
                 AdsInsights.Field.reach,
                 AdsInsights.Field.impressions,
                 AdsInsights.Field.actions,
                 AdsInsights.Field.action_values,
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
# nicosurge, alinker us, alinker ca, inabuggy, rvezy, nobilified
accounts_list = []


for account in accounts_list:
    logger.info(f'Beginning to sync {account}')
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
    logging.info("Accounts Table successfully synced to database")
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
    logging.info("Campaigns Table successfully synced to database")

    # AD SETS TABLE
    adsets_request = get_request(account_id=account,
                                 table='adsets',
                                 params=adset_params,
                                 fields=adset_fields
                                 )

    request_to_database(request=adsets_request,
                        table='adsets',
                        engine=engine
                        )
    logging.info("Ad Sets Table successfully synced to database")

    # ADS INSIGHTS TABLE
    # define an interval for batching with smaller date ranges:
    intv = 5
    end = datetime.strftime(datetime.now() - timedelta(days=1), "%Y-%m-%d")
    start = datetime.strftime(datetime.now() - timedelta(days=30), "%Y-%m-%d")
    time_ranges = batch_dates(start, end, intv)
    for i in range(intv):
        time_range = time_ranges[i]
        ads_params['time_range'] = time_range
        logging.info(f"batching from date range: {time_range['since']} - {time_range['until']}")
        ads_request = get_request(account_id=account,
                                  table='ads_insights',
                                  params=ads_params,
                                  fields=ads_fields
                                  )

        request_to_database(request=ads_request,
                            table='ads_insights',
                            engine=engine
                            )
        logging.info(f"batch success; {i+1} out of {intv}")
    logging.info("Ads Insights Table successfully synced to database")

    # AGE AND GENDER TABLE
    for i in range(intv):
        time_range = time_ranges[i]
        agegender_params['time_range'] = time_range
        logging.info(f"batching from date range: {time_range['since']} - {time_range['until']}")
        agegender_request = get_request(account_id=account,
                                        table='ads_insights_age_and_gender',
                                        params=agegender_params,
                                        fields=agegender_fields
                                        )

        request_to_database(request=agegender_request,
                            table='ads_insights_age_and_gender',
                            engine=engine
                            )
        logging.info(f"batch success; {i+1} out of {intv}")
        # WAIT 1 MINUTE
        sleeper(60)
    logging.info("Ads-Age and Gender Table successfully synced to database")

    # WAIT 5 MINUTES
    sleeper(300)

    # REGION TABLE
    for i in range(intv):
        time_range = time_ranges[i]
        region_params['time_range'] = time_range
        logging.info(f"batching from date range: {time_range['since']} - {time_range['until']}")
        region_request = get_request(account_id=account,
                                 table='ads_insights_region',
                                 params=region_params,
                                 fields=region_fields
                                 )

        request_to_database(request=region_request,
                            table='ads_insights_region',
                            engine=engine
                            )
        logging.info(f"batch success; {i+1} out of {intv}")
        # WAIT 1 MINUTE
        sleeper(60)
    logging.info("Ads-Region Table successfully synced to database")

    # END OF REQUESTS 
    logger.info(f'Completed syncing {account} to database')

