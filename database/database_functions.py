import json
import logging
import pandas as pd
import time
import yaml
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.campaign import Campaign
from facebook_business.api import FacebookAdsApi
from sqlalchemy.dialects.mysql import insert
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

def facebookconnect(secrets_path):
    """Connect to Facebook Marketing API.
    secrets_path: absolute path to client secrets;
    stored in json format.
    """
    with open(secrets_path) as authentication_file:
            authentication_result = json.load(authentication_file)
    # READ AUTHENTICATION JSON FILE AND EXTRACT
    my_app_id = authentication_result['my_app_id']
    my_app_secret = authentication_result['my_app_secret']
    my_access_token = authentication_result['my_access_token']
    # AUTHENTICATE FACEBOOK API CALL WITH APP/USER CREDENTIALS
    FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)

#++++++++++++++++++++
# MYSQL FUNCTION
#++++++++++++++++++++
# NEED TO INCLUDE BOTH TABLE NAME AND CLASS
def bulk_upsert(session, table, table_name,  df, id_cols):
    """Perform a bulk insert of the given list of mapping dictionaries.
    The bulk insert feature allows plain Python dictionaries to be used
    as the source of simple INSERT operations which can be more easily
    grouped together into higher performing “executemany” operations.
    Using dictionaries, there is no “history” or session state management
    features in use, reducing latency when inserting large numbers of
    simple rows.
    --------------------------------------------------------------------
    table: a mapped class (i.e. database table)
    table_name: the table name associated with table
    df: dataframe to be converted to a list of dictionaries
    id_col: a list of the primary keys in the table
    """
    primary_keys = ",".join(id_cols) # join PKs in string for query
    query = "SELECT " + primary_keys + " FROM " + table_name
    # store df of rows that exist in db and should be updated
    update_df = pd.read_sql_query(query, session.bind)
    print("query dataframe types: ")
    print(update_df.dtypes)
    merged_df = pd.merge(df, update_df, how='left', indicator=True)
    update_df = merged_df[merged_df['_merge']=='both'] # both exist
    update_df = update_df.drop(columns=['_merge'])
    # workaround for nans
    update_df = update_df.where(pd.notnull(update_df), None)
    print("update df:")
    print(update_df.info())
    # store df of rows that do not exist
    insert_df = merged_df[merged_df['_merge']=='left_only']
    insert_df = insert_df.drop(columns=['_merge'])
    # workaround for nans
    insert_df = insert_df.where(pd.notnull(insert_df), None)
    print("insert df:")
    print(insert_df.info())
    # after merge, the dataframes used as inputs for upserts
    # must be converted back to string objects, else
    # value error with timestamp...
    if 'date_start' in update_df:
        update_df['date_start'] = update_df['date_start'].astype(str)
        insert_df['date_start'] = insert_df['date_start'].astype(str)
        print("after string conversion:")
        print(insert_df.info())

    if not update_df.empty:
        num_updated = len(update_df.index)
        print(num_updated)
        update_df = update_df.to_dict(orient="records")
        session.bulk_update_mappings(
            table,
            update_df
        )
        logging.info('%s rows updated', num_updated)
    if not insert_df.empty:
        num_inserted = len(insert_df.index)
        print(num_inserted)
        num_inserted
        insert_df = insert_df.to_dict(orient="records")
        # insert any records that do not already exist in db
        session.bulk_insert_mappings(
            table,
            insert_df,
            render_nulls=True
        )
        logging.info('%s rows inserted', num_inserted)
        session.commit()

#++++++++++++++++++++++++
# DATAFRAME FUNCTIONS
#++++++++++++++++++++++++

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

#+++++++++++++++++++++++
# FACEBOOK API REQUESTS
#+++++++++++++++++++++++

def get_request(account_id, table, params, fields):

    my_account = AdAccount(account_id) # KOHO AD ACCOUNT
    if table == 'accounts':
        cursor = my_account.api_get(params=params,
                                    fields=fields)
        return dict(cursor)
    if table == 'campaigns':
        cursor = my_account.get_campaigns(params=params,
                                          fields=fields)
        request = [campaign for campaign in cursor]
        return  request
    if table == 'ads_insights':
        cursor = my_account.get_insights_async(params=params,
                                               fields=fields)
        cursor.api_get()
        while cursor[AdReportRun.Field.async_status] != "Job Completed":
            time.sleep(1)
            cursor.api_get()
            time.sleep(1)
        request = cursor.get_result(params={"limit": 1000})
        return request
    if table == 'ads_insights_age_and_gender':
        cursor = my_account.get_insights_async(params=params,
                                               fields=fields)
        cursor.api_get()
        while cursor[AdReportRun.Field.async_status] != "Job Completed":
            time.sleep(1)
            cursor.api_get()
            time.sleep(1)
        request = cursor.get_result(params={"limit": 1000})
        return request
    if table == 'ads_insights_region':
        cursor = my_account.get_insights_async(params=params,
                                               fields=fields)
        cursor.api_get()
        while cursor[AdReportRun.Field.async_status] != "Job Completed":
            time.sleep(1)
            cursor.api_get()
            time.sleep(1)
        request = cursor.get_result(params={"limit": 1000})
        return request
