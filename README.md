# Ingesting Data from Facebook Marketing API into a MySQL Database using the Python SDK
2019-09-19 created methods to batch requests into smaller date ranges; has improved overall performance and logging
* AdSetsTable created
* To do: create CountryTable, create AdsCreativeTable, create AdsMetaTable, create AdsCreativeMetricsTable

* 2019-09-13: project has been made into a package. Added tables:
* AdsInsightsAgeGenderTable
* AdsInsightsRegion Table
note: project ready to scale to include more accounts

2019-09-05: created models.py module where the database is to be engineered. Added tables:
* AccountsTable
* CampaignsTable
* AdsInsightsTable

## Introduction

This script was written using Python 3.7.3.
To make sure you are using the correct version to execute the script add the following lines of code to the .py file:
```{python}
import sys
print(sys.version)
```
Or simply `python --version` in the console.

## Python Main Prerequisites
To load requirements enter:
```
pip install -r requirements.txt
```

## Set up for Usage

You must have access to an existing mySQL database. Refer to https://docs.sqlalchemy.org/en/13/core/engines.html for engine configuration with SQLAlchemy. The syntax for the database Url is `dialect+driver://username:password@host:port/database `. I used PyMySQL as the preffered driver.

Next, execute the following in terminal:
```
mkdir staging; mkdir settings;
touch db_secrets.json; touch fb_client_secrets.json
```
The json files located in the settings directory should be filled your personal credentials in the following format:
1.  db_secrets.json:
```
{
    "hostname": "<YOUR HOST NAME>"
    "user": " <YOUR USER NAME>"
    "password": "<YOUR PASSWORD>"
}
```
2. fb_client_secrets.json: 
```
{
    "my_app_id: "<APP ID>"
    "my_app_secret": "<APP SECRET>"
    "my_access_token": "<ACCESS TOKEN>"
}
```
