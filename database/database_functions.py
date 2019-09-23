import json
import logging
import pandas as pd
import time
import yaml
from database.models import (
    mySQL_connect,
    AccountsTable,
    CampaignsTable,
    AdSetsTable,
    AdsInsightsTable,
    AdsInsightsAgeGenderTable,
    AdsInsightsRegionTable,
)
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
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
    account_id = df.account_id[0] # store the account id for query
    primary_keys = ",".join(id_cols) # join PKs in string for query
    query = "SELECT " + primary_keys + " FROM " + table_name + " WHERE account_id = '" + str(account_id) + "';"

    if 'date_start' in df:
        df['date_start'] = pd.to_datetime(df['date_start'])

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
        update_df.to_csv('database/staging/' + table_name + '_update.csv')
        num_updated = len(update_df.index)
        update_df = update_df.to_dict(orient="records")
        session.bulk_update_mappings(
            table,
            update_df
        )
        logger.info(f'{num_updated} rows updated in {table_name}')
    if not insert_df.empty:
        insert_df.to_csv('database/staging/' + table_name + '_insert.csv')
        num_inserted = len(insert_df.index)
        num_inserted
        insert_df = insert_df.to_dict(orient="records")
        # insert any records that do not already exist in db
        session.bulk_insert_mappings(
            table,
            insert_df,
            render_nulls=True
        )
        logger.info(f'{num_inserted} rows inserted in {table_name}')
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

def extract_col(row, action_type, attr_window='value'):
    """row: a row from a pandas df
    action_type: a type of action  -- eg: mobile app installs
    attr_window: the attribution window to search for
    returns: data associated with key, value pair
    """
    if type(row) != list:
        return 0
    else:
        index = find(lst=row, key='action_type', value=action_type)
        # index will be -1 if function cannot find value
        if index == -1:
            return 0
        try:
            return row[index][attr_window]
        except KeyError:
            return 0

def attribution_windows(df, nested_col, action_type, col):
    """(pandas df, str, str, str) -> pandas df
    Create separate columns for each attribution window
    nested_col is the name of the data frame column
    that contains all the data we wish to pull.
    col is the base name of column (e.g. purchases)
    """
    windows = ['1d_view', '7d_view', '28d_view',
               '1d_click', '7d_click', '28d_click']
    for win in windows:
        new_colname = col + '_' + win
        df[new_colname] = pd.to_numeric(
            df[nested_col].apply(
                extract_col, action_type=action_type,
                attr_window=win
            )
        )
    return df

def transform(df):
    """ Function to extract common columns and perform
    some manipulations (Transform stage)
    <-- takes a pandas dataframe
    --> returns pandas dataframe
    """
    # Landing Page View
    df = attribution_windows(df, 'actions', 'landing_page_view', 'landing_page_view')
    # Link Click
    df = attribution_windows(df, 'actions', 'link_click', 'link_click')
    # Posts
    df = attribution_windows(df, 'actions', 'post', 'post')
    # Page Engagement
    df = attribution_windows(df, 'actions', 'page_engagement', 'page_engagement')
    # Post Engagement
    df = attribution_windows(df, 'actions', 'post_engagement', 'post_engagement')
    # Add to cart
    df = attribution_windows(df, 'actions', 'omni_add_to_cart', 'add_to_cart')
    # Initiated Checkout
    df = attribution_windows(df, 'actions', 'omni_initiated_checkout', 'checkout')
    # Activate App
    df = attribution_windows(df, 'actions', 'omni_activate_app', 'app_starts')
    # App registrations
    df = attribution_windows(df, 'actions', 'omni_complete_registration', 'complete_registrations')
    # App Installs
    df = attribution_windows(df, 'actions', 'omni_app_install', 'app_install')
    # Purchase
    df = attribution_windows(df, 'actions', 'omni_purchase', 'purchase')

    # Renter Complete Registration
    df = attribution_windows(df, 'actions', 'offsite_conversion.custom.264800584268286', 'renter_complete_registration')
    # Renter Booking Sent
    df = attribution_windows(df, 'actions', 'offsite_conversion.custom.155619705306328', 'renter_booking_sent')
    # Owner Listed
    df = attribution_windows(df, 'actions', 'offsite_conversion.custom.2038839149667048', 'owner_listed')
    # Owner Complete Registration
    df = attribution_windows(df, 'actions', 'offsite_conversion.custom.1816163992024268', 'owner_complete_registration')


    # CONVERSION VALUES

    # Purchase
    df = attribution_windows(df, 'action_values', 'omni_purchase', 'purchase_value')

    # drop actions column
    df = df.drop(columns=['actions', 'action_values'])
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
    my_account = AdAccount(account_id)
    if table == 'accounts':
        cursor = my_account.api_get(params=params,
                                    fields=fields)
        return dict(cursor)
    if table == 'campaigns':
        cursor = my_account.get_campaigns(params=params,
                                          fields=fields)
        request = [campaign for campaign in cursor]
        return  request
    if table == 'adsets':
        request = my_account.get_ad_sets(params=params,
                                        fields=fields)
        return request
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
    else:
        df = pd.DataFrame(request,
                          columns = columns
                          ).astype(dtype=dtypes)

    # build session with MySQL
    Session = sessionmaker(bind=engine)
    session = Session()
    # dataframes inserted or updated into database
    if table == 'accounts':
        df.rename(columns={'name': 'account_name'},
                  inplace=True)
        bulk_upsert(session, table=AccountsTable,
                    table_name='accounts',
                    df = df, id_cols=['account_id'])

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
                   df=df, id_cols=['account_id', 'campaign_id'])

    if table == 'adsets':
        df.rename(columns={'id': 'adset_id',
                           'name': 'adset_name'},
                  inplace=True)
        df = df.where(pd.notnull(df), None)
        bulk_upsert(session, table=AdSetsTable,
                    table_name='adsets',
                    df=df, id_cols=['adset_id', 'account_id',
                                    'campaign_id'])

    if table == 'ads_insights':
        # campaign id may refer to a deleted campaign which
        # is not contained in the Campaigns table of the database
        # we keep only those ids which are;
        campaign_ids = session.query(CampaignsTable.campaign_id)
        campaign_ids = [i for i, in campaign_ids]
        # this also happens with deleted adsets
        adset_ids = session.query(AdSetsTable.adset_id)
        adset_ids = [i for i, in adset_ids]
        df = df.loc[df['campaign_id'].isin(campaign_ids), :]
        n = len(df.loc[df['adset_id'].isin(adset_ids)==False, :].index)
        if n > 0:
            logger.warning(f"{n} rows will not be synced - fk constraint")
            df.to_csv("database/staging/ignored_ads.csv")
        df = df.loc[df['adset_id'].isin(adset_ids), :]
        df = transform(df)
        df.to_csv("testing.csv")
        bulk_upsert(session, table=AdsInsightsTable,
                    table_name='ads_insights',
                    df=df, id_cols=['ad_id', 'account_id',
                                    'campaign_id', 'adset_id',
                                    'date_start'])

    if table == 'ads_insights_age_and_gender':
        campaign_ids = session.query(CampaignsTable.campaign_id)
        campaign_ids = [i for i, in campaign_ids]
        adset_ids = session.query(AdSetsTable.adset_id)
        adset_ids = [i for i, in adset_ids]
        df = df.loc[df['campaign_id'].isin(campaign_ids), :]
        df = df.loc[df['adset_id'].isin(adset_ids), :]
        df = transform(df)
        bulk_upsert(session, table=AdsInsightsAgeGenderTable,
                    table_name='ads_insights_age_and_gender',
                    df=df, id_cols=['ad_id', 'account_id',
                                    'campaign_id', 'adset_id',
                                    'date_start', 'age', 'gender'])

    if table == 'ads_insights_region':
        campaign_ids = session.query(CampaignsTable.campaign_id)
        campaign_ids = [i for i, in campaign_ids]
        adset_ids = session.query(AdSetsTable.adset_id)
        adset_ids = [i for i, in adset_ids]
        df = df.loc[df['campaign_id'].isin(campaign_ids), :]
        df = df.loc[df['adset_id'].isin(adset_ids), :]

        # have experienced duplicates in primary keys in this table
        # I will log them into a csv to keep track which were dropped
        duplicates = df[df.duplicated(subset=['ad_id', 'account_id', 'campaign_id', 'adset_id', 'date_start', 'region'], keep='first')]
        n = len(duplicates.index)
        if n > 0:
            logger.warning(f'{n} duplicate primary keys encountered: written to staging/duplicates.csv')
            duplicates.to_csv('database/staging/duplciates.csv')
            df.drop_duplicates(subset=['ad_id', 'account_id', 'campaign_id', 'adset_id', 'date_start', 'region'], keep = 'first', inplace = True)

        df = transform(df)
        bulk_upsert(session, table=AdsInsightsRegionTable,
                    table_name='ads_insights_region',
                    df=df, id_cols=['ad_id', 'account_id', 'campaign_id',
                                    'adset_id', 'date_start', 'region'])
    session.close()



def batch_dates(start, end, intv):
    """start and end define a date range in string format.
    Returns a list of dictionaries that can be used as parameters
    for a Facebook API request; breaking apart large date ranges
    into smaller ranges.
    """
    from datetime import datetime
    from datetime import timedelta
    def date_range(start, end, intv):
        """Take a date range and intv, return (intv) number of
        contiguous sub date ranges.
        e.g.: date_range('2019-09-17', '2019-10-01', 3)
        -> ['2019-09-17', '2019-09-21', '2019-09-26', '2019-10-01']
        """
        start = datetime.strptime(start, "%Y-%m-%d")
        end = datetime.strptime(end, "%Y-%m-%d")
        diff = (end - start) / intv
        for i in range(intv):
            yield (start+diff*i).strftime("%Y-%m-%d")
        yield end.strftime("%Y-%m-%d")
    new_ranges = list(date_range(start, end, intv)) # string formatted
    # date formatted
    new_dates = [datetime.strptime(x, "%Y-%m-%d") for x in new_ranges]
    # list of number of days differences between
    date_diffs = pd.Series(new_dates).diff().dt.days.iloc[1:].tolist()
    date_params = [] # list of dictionaries init
    for i in range(len(new_dates)-1):
        # on last date we want the range to include 'end' from args
        if i == (len(new_dates)-2):
            days = date_diffs[i]
        # otherwise less one day to prevent overlapping
        else:
            days = date_diffs[i]-1
        date_param = {
            'since': new_ranges[i],
            'until': datetime.strftime(
                new_dates[i] + timedelta(days=days),
                "%Y-%m-%d"
            )
        }
        date_params.append(date_param)
    return date_params


