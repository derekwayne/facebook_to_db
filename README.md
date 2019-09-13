# Ingesting Data from Facebook Marketing API into a MySQL Database using the Python SDK
2019-09-13: project has been made into a package. Added tables:
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
