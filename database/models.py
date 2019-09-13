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
    ForeignKey,
    DateTime,
    BigInteger,
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
    account_status = Column(String(16))
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
        PrimaryKeyConstraint('campaign_id'),
    )
    campaign_id = Column(BigInteger, primary_key=True,
                         autoincrement=False)
    campaign_name = Column(String(255))
    # Foreign Key for parent table accounts
    account_id = Column(BigInteger, ForeignKey('accounts.account_id'))
    effective_status = Column(String(16))
    updated_time = Column(DateTime)
    daily_budget = Column(Float)

class AdsInsightsTable(Base):
    __tablename__ = "ads_insights"
    __table_args__ = (
        PrimaryKeyConstraint('ad_id', 'date_start'),
    )
    ad_id = Column(BigInteger)
    campaign_id = Column(BigInteger, ForeignKey('campaigns.campaign_id'))
    date_start = Column(DateTime)
    ad_name = Column(String(255))
    campaign_name = Column(String(255))
    account_id = Column(BigInteger, ForeignKey('accounts.account_id'))
    frequency = Column(Float)
    cpc = Column(Float)
    cpm = Column(Float)
    spend = Column(Float)
    impressions = Column(BigInteger)
    ctr = Column(Float)
    bucket = Column(String(16))
    pr_rt = Column(String(5))
    mobile_app_installs = Column(Integer)
    registrations_completed = Column(Integer)
    clicks = Column(Integer)

class AdsInsightsAgeGenderTable(Base):
    __tablename__ = "ads_insights_age_and_gender"
    __table_args__ = (
        PrimaryKeyConstraint('ad_id', 'account_id',
                             'campaign_id', 'date_start',
                             'age', 'gender'),
    )
    ad_id = Column(BigInteger, ForeignKey('ads_insights.ad_id'))
    account_id = Column(BigInteger, ForeignKey('accounts.account_id'))
    campaign_id = Column(BigInteger, ForeignKey('campaigns.campaign_id'))
    date_start = Column(DateTime)
    age = Column(String(7))
    gender = Column(String(10))
    frequency = Column(Float)
    cpc = Column(Float)
    cpm = Column(Float)
    spend = Column(Float)
    impressions = Column(BigInteger)
    ctr = Column(Float)
    mobile_app_installs = Column(Integer)
    registrations_completed = Column(Integer)
    clicks = Column(Integer)

class AdsInsightsRegionTable(Base):
    __tablename__ = "ads_insights_region"
    __table_args__ = (
        PrimaryKeyConstraint('ad_id', 'account_id',
                             'campaign_id', 'date_start',
                             'region'),
    )
    ad_id = Column(BigInteger, ForeignKey('ads_insights.ad_id'))
    account_id = Column(BigInteger, ForeignKey('accounts.account_id'))
    campaign_id = Column(BigInteger, ForeignKey('campaigns.campaign_id'))
    date_start = Column(DateTime)
    region = Column(String(45))
    frequency = Column(Float)
    cpc = Column(Float)
    cpm = Column(Float)
    spend = Column(Float)
    impressions = Column(BigInteger)
    ctr = Column(Float)
    mobile_app_installs = Column(Integer)
    registrations_completed = Column(Integer)
    clicks = Column(Integer)

credentials_path = 'database/settings/db_secrets.json'
engine = mySQL_connect(credentials_path, port='3306', db='test_schema')
AccountsTable.__table__.create(bind=engine, checkfirst=True)
CampaignsTable.__table__.create(bind=engine, checkfirst=True)
AdsInsightsTable.__table__.create(bind=engine, checkfirst=True)
AdsInsightsAgeGenderTable.__table__.create(bind=engine, checkfirst=True)
AdsInsightsRegionTable.__table__.create(bind=engine, checkfirst=True)
##
