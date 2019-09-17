import json
import logging
import pandas as pd
import time
import yaml
from database.models import (
    mySQL_connect,
    AccountsTable,
    CampaignsTable,
    AdsInsightsTable,
    AdsInsightsAgeGenderTable,
    AdsInsightsRegionTable,
)
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
with open('database/config.yaml', 'r') as f:
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
    merged_df = pd.merge(df, update_df, how='left', indicator=True)
    update_df = merged_df[merged_df['_merge']=='both'] # both exist
    update_df = update_df.drop(columns=['_merge'])
    # workaround for nans
    update_df = update_df.where(pd.notnull(update_df), None)

    # store df of rows that do not exist
    insert_df = merged_df[merged_df['_merge']=='left_only']
    insert_df = insert_df.drop(columns=['_merge'])
    # workaround for nans
    insert_df = insert_df.where(pd.notnull(insert_df), None)

    # after merge, the dataframes used as inputs for upserts
    # must be converted back to string objects, else
    # value error with timestamp...
    if 'date_start' in update_df:
        update_df['date_start'] = update_df['date_start'].astype(str)
        insert_df['date_start'] = insert_df['date_start'].astype(str)

    if not update_df.empty:
        update_df.to_csv('staging/' + table_name + '.csv')
        logging.info('Staging table created for {table_name}')
        num_updated = len(update_df.index)
        update_df = update_df.to_dict(orient="records")
        session.bulk_update_mappings(
            table,
            update_df
        )
        logging.info('{num_updated} rows updated in {table_name}')
    if not insert_df.empty:
        num_inserted = len(insert_df.index)
        num_inserted
        insert_df = insert_df.to_dict(orient="records")
        # insert any records that do not already exist in db
        session.bulk_insert_mappings(
            table,
            insert_df,
            render_nulls=True
        )
        logging.info('{num_inserted} rows inserted in {table_name}')
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

def transform(df):
    """ Function to extract common columns and perform
    some manipulations (Transform stage)
    <-- takes a pandas dataframe
    --> returns pandas dataframe
    """
    # App Installs
    df['mobile_app_installs'] = pd.to_numeric(
        df['actions'].apply(
            extract_col, value='mobile_app_install'
        )
    )
    # Registrations
    df['registrations_completed'] = pd.to_numeric(
        df['actions'].apply(
            extract_col,
            value='app_custom_event.fb_mobile_complete_registration'
        )
    )
    # Link Clicks
    df['clicks'] = pd.to_numeric(
        df['actions'].apply(extract_col, value='link_click')
    )
    df['date_start'] = pd.to_datetime(df['date_start'])
    df = df.drop(columns=['actions'])
    return df

#+++++++++++++++++++++++
# FACEBOOK API REQUESTS
#+++++++++++++++++++++++

def get_request(account_id, table, params, fields):
    """account_id: unique id for ad account in format act_<ID>
    table: The table object found in the models module
    params: dictionary of parameters for request
    fields: list of fields for request
    --> returns requested data from Facebook Marketing API
    """
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
    with open('database/columns/' + table + '.json') as f:
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
        bulk_upsert(session, table=AccountsTable,
                    table_name='accounts',
                    df = df, id_cols=['account_id'])
        logger.info('Accounts table has been synced to database')

    if table == 'campaigns':
        # must rename these columns due to Field class attributes
        # from parent Campaign (see facebook-business)
        df.rename(columns={'id': 'campaign_id',
                           'name': 'campaign_name'},
                  inplace=True)
        # nans throw errors so swap with None
        df = df.where(pd.notnull(df), None)
        bulk_upsert(session, table=CampaignsTable,
                    table_name='campaigns',
                   df=df, id_cols=['campaign_id'])
        logger.info('Campaigns table has been synced to database')

    if table == 'ads_insights':
        # campaign id may refer to a deleted campaign which
        # is not contained in the Campaigns table of the database
        # we keep only those ids which are;
        campaign_ids = session.query(CampaignsTable.campaign_id)
        campaign_ids = [i for i, in campaign_ids]
        df = df.loc[df['campaign_id'].isin(campaign_ids), :]
        df = transform(df)
        df.to_csv('database/data/' + table + '.csv')
        bulk_upsert(session, table=AdsInsightsTable,
                    table_name='ads_insights',
                    df=df, id_cols=['ad_id', 'date_start'])
        logger.info('Ads Insights table has been synced to database')

    if table == 'ads_insights_age_and_gender':
        campaign_ids = session.query(CampaignsTable.campaign_id)
        campaign_ids = [i for i, in campaign_ids]
        df = df.loc[df['campaign_id'].isin(campaign_ids), :]
        df = transform(df)
        df.to_csv('database/data/' + table + '.csv')
        bulk_upsert(session, table=AdsInsightsAgeGenderTable,
                    table_name='ads_insights_age_and_gender',
                    df=df, id_cols=['ad_id', 'account_id',
                                    'campaign_id', 'date_start',
                                    'age', 'gender'])
        logger.info('Ads Insights Age and Gender table has been synced to database')

    if table == 'ads_insights_region':
        campaign_ids = session.query(CampaignsTable.campaign_id)
        campaign_ids = [i for i, in campaign_ids]
        df = df.loc[df['campaign_id'].isin(campaign_ids), :]
        df = transform(df)
        df.to_csv('database/data/' + table + '.csv')
        bulk_upsert(session, table=AdsInsightsRegionTable,
                    table_name='ads_insights_region',
                    df=df, id_cols=['ad_id', 'account_id', 'campaign_id',
                                    'date_start', 'region'])
        logger.info('Ads Insights Region table has been synced to database')
    df.to_csv('database/data/' + table + '.csv')
    session.close()

