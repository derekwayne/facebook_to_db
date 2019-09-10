import json
import pandas as pd
import time

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.campaign import Campaign
from facebook_business.api import FacebookAdsApi
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.orm import sessionmaker

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
