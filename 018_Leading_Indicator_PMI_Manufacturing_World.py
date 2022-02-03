import requests
import calendar
import re
import pandas as pd
import xml.etree.ElementTree as ET
from inspect import getmembers, isclass, isfunction
from datetime import datetime as dt
from dateutil import parser, relativedelta
from datetime import date
from bs4 import BeautifulSoup
from requests.models import parse_header_links
from common import convert_excelsheet_to_dataframe, write_dataframe_to_excel
from common import combine_df_on_index, get_yf_data, get_gdp_fred
from common import get_ism_manufacturing_content, scrape_ism_manufacturing_headline_index

excel_file_path = '/Trading_Excel_Files/03_Leading_Indicators/018_Leading_Indicator_PMI_Manufacturing_World.xlsm'

def scrape_table_country_pmi():

    url = "https://tradingeconomics.com/country-list/manufacturing-pmi"

    page = requests.get(url=url)
    soup = BeautifulSoup(page.content, 'html.parser')

    data = {'Date': []}
    
    # Convert the dictionary into DataFrame
    df_countries_pmi = pd.DataFrame(data)

    #TODO: Need to scrape table for world production countries and numbers.
    table = soup.find('table')

    table_rows = table.find_all('tr', recursive=False)

    #data = {country: [], "Date": []}
    df = pd.DataFrame()
    index = 0

    #Get rows of data.
    for tr in table_rows:
        temp_row = []
        index = 0
        country = ""
        td = tr.find_all('td')

        for obs in td:
            text = str(obs.text).strip()
            if(index == 0):
                country = text
            if(index == 1):
                value = text
                temp_row.append(text)
            if(index == 3):
                dt_date = dt.strptime(text,'%b/%y')
                text = dt_date.strftime('%b-%y')
                temp_row.append(text)
            index += 1

        if(temp_row):
            data2 = {country: [], "Date": []}
            df_country_pmi = pd.DataFrame(data2)
            df_country_pmi.loc[len(df_country_pmi.index)] = temp_row

            df_country_pmi["Date"] = pd.to_datetime(df_country_pmi["Date"], format='%b-%y')
            df_country_pmi[country] = pd.to_numeric(df_country_pmi[country])

            # Combine Date and PMI with existing df_countries_pmi
            df_countries_pmi = combine_df_on_index(df_countries_pmi, df_country_pmi,'Date')

            print("Retrieved Data For: %s - %s" % (country, temp_row))

    return df_countries_pmi

def scrape_china_official_pmi():
    url = "https://tradingeconomics.com/china/business-confidence"

    page = requests.get(url=url)
    soup = BeautifulSoup(page.content, 'html.parser')

    data = {'China Business Confidence': [],'DATE': []}
    
    # Convert the dictionary into DataFrame
    df_china_official_pmi = pd.DataFrame(data)

    table = soup.find_all('table')
    table_rows = table[1].find_all('tr', recursive=False)
    temp_row = []

    #Get rows of data.
    for tr in table_rows:
        td = tr.find_all('td')

        if(td[0].text.strip() == 'Business Confidence'):        
            index = 0
    
            for obs in td:
                text = str(obs.text).strip()
                if(index == 1):
                    value = text
                    temp_row.append(text)
                if(index == 4):
                    dt_date = dt.strptime(text,'%b %Y')
                    text = dt_date.strftime('%b-%y')
                    temp_row.append(text)
                index += 1

    if(temp_row):
        df_china_official_pmi.loc[len(df_china_official_pmi.index)] = temp_row

        df_china_official_pmi["DATE"] = pd.to_datetime(df_china_official_pmi["DATE"], format='%b-%y')
        df_china_official_pmi["China Business Confidence"] = pd.to_numeric(df_china_official_pmi["China Business Confidence"])

    return df_china_official_pmi

#####################################################
# Get PMI Index for Countries from TradingEconomics #
#####################################################

sheet_name = 'DB Country PMI'

#Get Country Rankings
df_countries_pmi = scrape_table_country_pmi()

# Load original data from excel file into original df
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

#Combine new data with original data
df_updated = combine_df_on_index(df_original, df_countries_pmi, 'Date')

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

####################################
# Get ACWI, EWU, EZU Index from YF #
####################################

sheet_name = 'DB Global PMI'

#get date range
todays_date = date.today()
date_str = "%s-%s-01" % (todays_date.year, todays_date.month)

df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

# Get EUR/USD close day intervals using above date range
df_ACWI = get_yf_data("ACWI", "1mo", "2010-10-01", date_str)

#Remove unnecessary columns from df_ACWI and rename columns
df_ACWI = df_ACWI.drop(['Open', 'High', 'Low', 'Volume'], axis=1)
df_ACWI = df_ACWI.rename(columns={"Close": "ACWI"})
df_ACWI = df_ACWI.dropna()

#Combine new data with original data
df_updated = combine_df_on_index(df_original, df_ACWI, 'DATE')

# Get EZU close monthly intervals
df_EZU = get_yf_data("EZU", "1mo", "2004-12-01", date_str)
#Remove unnecessary columns from df_EZU and rename columns
df_EZU = df_EZU.drop(['Open', 'High', 'Low', 'Volume'], axis=1)
df_EZU = df_EZU.rename(columns={"Close": "EZU"})
df_EZU = df_EZU.dropna()

#Combine new data with original data
df_updated = combine_df_on_index(df_updated, df_EZU, 'DATE')

# Get EWU close monthly intervals
df_EWU = get_yf_data("EWU", "1mo", "2004-12-01", date_str)
#Remove unnecessary columns from df_EWU and rename columns
df_EWU = df_EWU.drop(['Open', 'High', 'Low', 'Volume'], axis=1)
df_EWU = df_EWU.rename(columns={"Close": "EWU"})
df_EWU = df_EWU.dropna()

#Combine new data with original data
df_updated = combine_df_on_index(df_updated, df_EWU, 'DATE')

# Reorder Columns
# get a list of columns
cols = list(df_updated)
# move the column to head of list using index, pop and insert
cols.insert(0, cols.pop(cols.index('DATE')))
cols.insert(1, cols.pop(cols.index('Global')))
cols.insert(2, cols.pop(cols.index('ACWI')))
cols.insert(3, cols.pop(cols.index('EZU')))
cols.insert(4, cols.pop(cols.index('EWU')))

# reorder
df_updated = df_updated[cols]

write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

############################
# Get US ISM Manufacturing #
############################

sheet_name = 'DB US ISM Manufacturing'
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

ism_date, ism_month, page = get_ism_manufacturing_content()
df_ism_headline_index = scrape_ism_manufacturing_headline_index(ism_date, ism_month)

#Drop unnecessary columns
df_ism_headline_index = df_ism_headline_index.drop(columns='NEW_ORDERS', axis=1)
df_ism_headline_index = df_ism_headline_index.drop(columns='IMPORTS', axis=1)
df_ism_headline_index = df_ism_headline_index.drop(columns='BACKLOG_OF_ORDERS', axis=1)
df_ism_headline_index = df_ism_headline_index.drop(columns='PRICES', axis=1)
df_ism_headline_index = df_ism_headline_index.drop(columns='PRODUCTION', axis=1)
df_ism_headline_index = df_ism_headline_index.drop(columns='CUSTOMERS_INVENTORIES', axis=1)
df_ism_headline_index = df_ism_headline_index.drop(columns='INVENTORIES', axis=1)
df_ism_headline_index = df_ism_headline_index.drop(columns='DELIVERIES', axis=1)
df_ism_headline_index = df_ism_headline_index.drop(columns='EMPLOYMENT', axis=1)
df_ism_headline_index = df_ism_headline_index.drop(columns='EXPORTS', axis=1)

#Rename column
df_ism_headline_index = df_ism_headline_index.rename(columns={"ISM": "ISM Manufacturing"})

#Combine new data with original data
df_updated = combine_df_on_index(df_original, df_ism_headline_index, 'DATE')

write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

##########################
# Get China Official PMI #
##########################

sheet_name = 'DB China Official PMI'
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

df_china_official_pmi = scrape_china_official_pmi()

#Combine new data with original data
df_updated = combine_df_on_index(df_original, df_china_official_pmi, 'DATE')

write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

#########################
# Get Euro Area GDP QoQ #
#########################

sheet_name = 'DB Global PMI'

df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

series_name = 'CLVMEURSCAB1GQEA19'

df_EuroAreaGDP = get_gdp_fred(series_name)

#Drop unnecessary columns
df_EuroAreaGDP = df_EuroAreaGDP.drop(columns=series_name, axis=1)
df_EuroAreaGDP = df_EuroAreaGDP.drop(columns='GDPYoY', axis=1)
df_EuroAreaGDP = df_EuroAreaGDP.drop(columns='GDPQoQ_ANNUALIZED', axis=1)

#Rename column
df_EuroAreaGDP = df_EuroAreaGDP.rename(columns={"GDPQoQ": "EuroGDPQoQ"})

#Combine new data with original data
df_updated = combine_df_on_index(df_original, df_EuroAreaGDP, 'DATE')

# Reorder Columns
# get a list of columns
cols = list(df_updated)
# move the column to head of list using index, pop and insert
cols.insert(0, cols.pop(cols.index('DATE')))
cols.insert(1, cols.pop(cols.index('Global')))
cols.insert(2, cols.pop(cols.index('ACWI')))
cols.insert(3, cols.pop(cols.index('EZU')))
cols.insert(4, cols.pop(cols.index('EWU')))
cols.insert(5, cols.pop(cols.index('EuroGDPQoQ')))

# reorder
df_updated = df_updated[cols]

write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

print("Done!")