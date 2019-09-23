##
import pymysql
import json
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Sequence,
    Float,
    PrimaryKeyConstraint,
    ForeignKeyConstraint,
    ForeignKey,
    DateTime,
    BigInteger,
    Unicode,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, backref
from sqlalchemy.sql import *
##
#+++++++++++++++++++++++++++++++++++++++
# AWS MYSQL-ENGINE
#+++++++++++++++++++++++++++++++++++++++

def mySQL_connect(credentials_path, port, db):
    with open(credentials_path) as authentication_file:
        authentication_result = json.load(authentication_file)
    engine = create_engine(
        "mysql+pymysql://{user}:{passwd}@{host}:{port}/{database}".format(
            user=authentication_result['user'],
            passwd=authentication_result['password'],
            host=authentication_result['hostname'],
            port=port,
            database=db
        )
    )
    return(engine)

##
#++++++++++++++++++++++++++++++++++++++
# DATABASE MODEL
#++++++++++++++++++++++++++++++++++++++
##

Base = declarative_base() # Class inheritance

class AccountsTable(Base):
    __tablename__ = "accounts"
    account_id = Column(BigInteger, primary_key=True,
                        autoincrement=False)
    account_name = Column(String(45))
    account_status = Column(Integer)
    currency = Column(String(5))
    amount_spent = Column(Float)
    # 1-N relations
    campaigns = relationship('CampaignsTable',
                             backref = 'accounts',
                             lazy = True,
                             cascade = 'all, delete-orphan'
                             )

class CampaignsTable(Base):
    __tablename__ = "campaigns"
    __table_args__ = (
        PrimaryKeyConstraint('campaign_id', 'account_id'),
    )
    campaign_id = Column(BigInteger)
    campaign_name = Column(String(255))
    # Foreign Key for parent table accounts
    account_id = Column(BigInteger,
                        ForeignKey('accounts.account_id',
                        onupdate='CASCADE',
                        ondelete='CASCADE')
                        )
    effective_status = Column(String(16))
    updated_time = Column(DateTime)
    daily_budget = Column(Float)
    objective = Column(String(45))

class AdSetsTable(Base):
    __tablename__ = "adsets"
    __table_args__ = (
        PrimaryKeyConstraint('account_id', 'campaign_id', 'adset_id'),
        ForeignKeyConstraint(
            ['account_id', 'campaign_id'],
            ['campaigns.account_id', 'campaigns.campaign_id'],
            onupdate="CASCADE", ondelete="CASCADE")
    )
    adset_id = Column(BigInteger)
    account_id = Column(BigInteger)
    campaign_id = Column(BigInteger)
    adset_name = Column(String(255))
    created_time = Column(DateTime)
    daily_budget = Column(Float)
    status = Column(String(16))
    optimization_goal = Column(String(45))
    updated_time = Column(DateTime)

class AdsInsightsTable(Base):
    __tablename__ = "ads_insights"
    __table_args__ = (
        PrimaryKeyConstraint('ad_id', 'account_id', 'campaign_id',
                             'adset_id', 'date_start'),
        ForeignKeyConstraint(
            ['account_id', 'campaign_id', 'adset_id'],
            ['adsets.account_id', 'adsets.campaign_id', 'adsets.adset_id'],
            onupdate="CASCADE", ondelete="CASCADE")
    )
    ad_id = Column(BigInteger)
    account_id = Column(BigInteger)
    campaign_id = Column(BigInteger)
    adset_id = Column(BigInteger)
    date_start = Column(DateTime)
    ad_name = Column(String(255))
    account_name = Column(String(45))
    frequency = Column(Float)
    reach = Column(Integer)
    link_click_1d_view = Column(Integer)
    link_click_7d_view = Column(Integer)
    link_click_28d_view = Column(Integer)
    link_click_1d_click = Column(Integer)
    link_click_7d_click = Column(Integer)
    link_click_28d_click = Column(Integer)
    add_to_cart_1d_view = Column(Integer)
    add_to_cart_7d_view = Column(Integer)
    add_to_cart_28d_view = Column(Integer)
    add_to_cart_1d_click = Column(Integer)
    add_to_cart_7d_click = Column(Integer)
    add_to_cart_28d_click = Column(Integer)
    checkout_1d_view = Column(Integer)
    checkout_7d_view = Column(Integer)
    checkout_28d_view = Column(Integer)
    checkout_1d_click = Column(Integer)
    checkout_7d_click = Column(Integer)
    checkout_28d_click = Column(Integer)
    app_starts_1d_view = Column(Integer)
    app_starts_7d_view = Column(Integer)
    app_starts_28d_view = Column(Integer)
    app_starts_1d_click = Column(Integer)
    app_starts_7d_click = Column(Integer)
    app_starts_28d_click = Column(Integer)
    complete_registrations_1d_view = Column(Integer)
    complete_registrations_7d_view = Column(Integer)
    complete_registrations_28d_view = Column(Integer)
    complete_registrations_1d_click = Column(Integer)
    complete_registrations_7d_click = Column(Integer)
    complete_registrations_28d_click = Column(Integer)
    app_install_1d_view = Column(Integer)
    app_install_7d_view = Column(Integer)
    app_install_28d_view = Column(Integer)
    app_install_1d_click = Column(Integer)
    app_install_7d_click = Column(Integer)
    app_install_28d_click = Column(Integer)
    purchase_1d_view = Column(Integer)
    purchase_7d_view = Column(Integer)
    purchase_28d_view = Column(Integer)
    purchase_1d_click = Column(Integer)
    purchase_7d_click = Column(Integer)
    purchase_28d_click = Column(Integer)
    purchase_value_1d_view = Column(Float)
    purchase_value_7d_view = Column(Float)
    purchase_value_28d_view = Column(Float)
    purchase_value_1d_click = Column(Float)
    purchase_value_7d_click = Column(Float)
    purchase_value_28d_click = Column(Float)
    spend = Column(Float)
    impressions = Column(BigInteger)
    renter_complete_registration_1d_view = Column(Integer)
    renter_complete_registration_7d_view = Column(Integer)
    renter_complete_registration_28d_view = Column(Integer)
    renter_complete_registration_1d_click = Column(Integer)
    renter_complete_registration_7d_click = Column(Integer)
    renter_complete_registration_28d_click = Column(Integer)
    renter_booking_sent_1d_view = Column(Integer)
    renter_booking_sent_7d_view = Column(Integer)
    renter_booking_sent_28d_view = Column(Integer)
    renter_booking_sent_1d_click = Column(Integer)
    renter_booking_sent_7d_click = Column(Integer)
    renter_booking_sent_28d_click = Column(Integer)
    owner_complete_registration_1d_view = Column(Integer)
    owner_complete_registration_7d_view = Column(Integer)
    owner_complete_registration_28d_view = Column(Integer)
    owner_complete_registration_1d_click = Column(Integer)
    owner_complete_registration_7d_click = Column(Integer)
    owner_complete_registration_28d_click = Column(Integer)
    owner_listed_1d_view = Column(Integer)
    owner_listed_7d_view = Column(Integer)
    owner_listed_28d_view = Column(Integer)
    owner_listed_1d_click = Column(Integer)
    owner_listed_7d_click = Column(Integer)
    owner_listed_28d_click = Column(Integer)


class AdsInsightsAgeGenderTable(Base):
    __tablename__ = "ads_insights_age_and_gender"
    __table_args__ = (
        PrimaryKeyConstraint('ad_id', 'account_id',
                             'campaign_id', 'adset_id',
                             'date_start', 'age', 'gender'),
        ForeignKeyConstraint(
            ['account_id', 'campaign_id', 'adset_id', 'ad_id'],
            ['ads_insights.account_id', 'ads_insights.campaign_id',
             'ads_insights.adset_id', 'ads_insights.ad_id'],
            onupdate="CASCADE", ondelete="CASCADE")
    )
    ad_id = Column(BigInteger)
    account_id = Column(BigInteger)
    campaign_id = Column(BigInteger)
    adset_id = Column(BigInteger)
    date_start = Column(DateTime)
    age = Column(String(7))
    gender = Column(String(10))
    frequency = Column(Float)
    reach = Column(Integer)
    link_click_1d_view = Column(Integer)
    link_click_7d_view = Column(Integer)
    link_click_28d_view = Column(Integer)
    link_click_1d_click = Column(Integer)
    link_click_7d_click = Column(Integer)
    link_click_28d_click = Column(Integer)
    add_to_cart_1d_view = Column(Integer)
    add_to_cart_7d_view = Column(Integer)
    add_to_cart_28d_view = Column(Integer)
    add_to_cart_1d_click = Column(Integer)
    add_to_cart_7d_click = Column(Integer)
    add_to_cart_28d_click = Column(Integer)
    checkout_1d_view = Column(Integer)
    checkout_7d_view = Column(Integer)
    checkout_28d_view = Column(Integer)
    checkout_1d_click = Column(Integer)
    checkout_7d_click = Column(Integer)
    checkout_28d_click = Column(Integer)
    app_starts_1d_view = Column(Integer)
    app_starts_7d_view = Column(Integer)
    app_starts_28d_view = Column(Integer)
    app_starts_1d_click = Column(Integer)
    app_starts_7d_click = Column(Integer)
    app_starts_28d_click = Column(Integer)
    complete_registrations_1d_view = Column(Integer)
    complete_registrations_7d_view = Column(Integer)
    complete_registrations_28d_view = Column(Integer)
    complete_registrations_1d_click = Column(Integer)
    complete_registrations_7d_click = Column(Integer)
    complete_registrations_28d_click = Column(Integer)
    app_install_1d_view = Column(Integer)
    app_install_7d_view = Column(Integer)
    app_install_28d_view = Column(Integer)
    app_install_1d_click = Column(Integer)
    app_install_7d_click = Column(Integer)
    app_install_28d_click = Column(Integer)
    purchase_1d_view = Column(Integer)
    purchase_7d_view = Column(Integer)
    purchase_28d_view = Column(Integer)
    purchase_1d_click = Column(Integer)
    purchase_7d_click = Column(Integer)
    purchase_28d_click = Column(Integer)
    purchase_value_1d_view = Column(Float)
    purchase_value_7d_view = Column(Float)
    purchase_value_28d_view = Column(Float)
    purchase_value_1d_click = Column(Float)
    purchase_value_7d_click = Column(Float)
    purchase_value_28d_click = Column(Float)
    spend = Column(Float)
    impressions = Column(BigInteger)
    renter_complete_registration_1d_view = Column(Integer)
    renter_complete_registration_7d_view = Column(Integer)
    renter_complete_registration_28d_view = Column(Integer)
    renter_complete_registration_1d_click = Column(Integer)
    renter_complete_registration_7d_click = Column(Integer)
    renter_complete_registration_28d_click = Column(Integer)
    renter_booking_sent_1d_view = Column(Integer)
    renter_booking_sent_7d_view = Column(Integer)
    renter_booking_sent_28d_view = Column(Integer)
    renter_booking_sent_1d_click = Column(Integer)
    renter_booking_sent_7d_click = Column(Integer)
    renter_booking_sent_28d_click = Column(Integer)
    owner_complete_registration_1d_view = Column(Integer)
    owner_complete_registration_7d_view = Column(Integer)
    owner_complete_registration_28d_view = Column(Integer)
    owner_complete_registration_1d_click = Column(Integer)
    owner_complete_registration_7d_click = Column(Integer)
    owner_complete_registration_28d_click = Column(Integer)
    owner_listed_1d_view = Column(Integer)
    owner_listed_7d_view = Column(Integer)
    owner_listed_28d_view = Column(Integer)
    owner_listed_1d_click = Column(Integer)
    owner_listed_7d_click = Column(Integer)
    owner_listed_28d_click = Column(Integer)

class AdsInsightsRegionTable(Base):
    __tablename__ = "ads_insights_region"
    __table_args__ = (
        PrimaryKeyConstraint('ad_id', 'account_id',
                             'campaign_id', 'adset_id',
                             'date_start', 'region'),
        ForeignKeyConstraint(
            ['account_id', 'campaign_id', 'adset_id', 'ad_id'],
            ['ads_insights.account_id', 'ads_insights.campaign_id',
             'ads_insights.adset_id', 'ads_insights.ad_id'],
            onupdate="CASCADE", ondelete="CASCADE")
    )
    ad_id = Column(BigInteger)
    account_id = Column(BigInteger)
    campaign_id = Column(BigInteger)
    adset_id = Column(BigInteger)
    date_start = Column(DateTime)
    region = Column(Unicode(45, collation='utf8_general_ci'))
    frequency = Column(Float)
    reach = Column(Integer)
    link_click_1d_view = Column(Integer)
    link_click_7d_view = Column(Integer)
    link_click_28d_view = Column(Integer)
    link_click_1d_click = Column(Integer)
    link_click_7d_click = Column(Integer)
    link_click_28d_click = Column(Integer)
    add_to_cart_1d_view = Column(Integer)
    add_to_cart_7d_view = Column(Integer)
    add_to_cart_28d_view = Column(Integer)
    add_to_cart_1d_click = Column(Integer)
    add_to_cart_7d_click = Column(Integer)
    add_to_cart_28d_click = Column(Integer)
    checkout_1d_view = Column(Integer)
    checkout_7d_view = Column(Integer)
    checkout_28d_view = Column(Integer)
    checkout_1d_click = Column(Integer)
    checkout_7d_click = Column(Integer)
    checkout_28d_click = Column(Integer)
    app_starts_1d_view = Column(Integer)
    app_starts_7d_view = Column(Integer)
    app_starts_28d_view = Column(Integer)
    app_starts_1d_click = Column(Integer)
    app_starts_7d_click = Column(Integer)
    app_starts_28d_click = Column(Integer)
    complete_registrations_1d_view = Column(Integer)
    complete_registrations_7d_view = Column(Integer)
    complete_registrations_28d_view = Column(Integer)
    complete_registrations_1d_click = Column(Integer)
    complete_registrations_7d_click = Column(Integer)
    complete_registrations_28d_click = Column(Integer)
    app_install_1d_view = Column(Integer)
    app_install_7d_view = Column(Integer)
    app_install_28d_view = Column(Integer)
    app_install_1d_click = Column(Integer)
    app_install_7d_click = Column(Integer)
    app_install_28d_click = Column(Integer)
    purchase_1d_view = Column(Integer)
    purchase_7d_view = Column(Integer)
    purchase_28d_view = Column(Integer)
    purchase_1d_click = Column(Integer)
    purchase_7d_click = Column(Integer)
    purchase_28d_click = Column(Integer)
    purchase_value_1d_view = Column(Float)
    purchase_value_7d_view = Column(Float)
    purchase_value_28d_view = Column(Float)
    purchase_value_1d_click = Column(Float)
    purchase_value_7d_click = Column(Float)
    purchase_value_28d_click = Column(Float)
    spend = Column(Float)
    impressions = Column(BigInteger)
    renter_complete_registration_1d_view = Column(Integer)
    renter_complete_registration_7d_view = Column(Integer)
    renter_complete_registration_28d_view = Column(Integer)
    renter_complete_registration_1d_click = Column(Integer)
    renter_complete_registration_7d_click = Column(Integer)
    renter_complete_registration_28d_click = Column(Integer)
    renter_booking_sent_1d_view = Column(Integer)
    renter_booking_sent_7d_view = Column(Integer)
    renter_booking_sent_28d_view = Column(Integer)
    renter_booking_sent_1d_click = Column(Integer)
    renter_booking_sent_7d_click = Column(Integer)
    renter_booking_sent_28d_click = Column(Integer)
    owner_complete_registration_1d_view = Column(Integer)
    owner_complete_registration_7d_view = Column(Integer)
    owner_complete_registration_28d_view = Column(Integer)
    owner_complete_registration_1d_click = Column(Integer)
    owner_complete_registration_7d_click = Column(Integer)
    owner_complete_registration_28d_click = Column(Integer)
    owner_listed_1d_view = Column(Integer)
    owner_listed_7d_view = Column(Integer)
    owner_listed_28d_view = Column(Integer)
    owner_listed_1d_click = Column(Integer)
    owner_listed_7d_click = Column(Integer)
    owner_listed_28d_click = Column(Integer)

credentials_path = 'database/settings/db_secrets.json' # add database/ back
engine = mySQL_connect(credentials_path, port='3306', db='acquire')
Base.metadata.create_all(bind=engine, checkfirst=True)
##
