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
from common import combine_df_on_index, get_yf_data, _util_check_diff_list, _transform_data
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

##########################
# Get ACWI Index from YF #
##########################

sheet_name = 'DB Global PMI'
df_original_ACWI = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

#get date range
todays_date = date.today()
date_str = "%s-%s-%s" % (todays_date.year, todays_date.month, todays_date.day)

# Get EUR/USD close day intervals using above date range
df_ACWI = get_yf_data("ACWI", "1mo", "2010-10-01", date_str)

#Remove unnecessary columns from df_ACWI and rename columns
df_ACWI = df_ACWI.drop(['Open', 'High', 'Low', 'Volume'], axis=1)
df_ACWI = df_ACWI.rename(columns={"Close": "ACWI"})
df_ACWI = df_ACWI.dropna()

#Combine new data with original data
df_updated = combine_df_on_index(df_original_ACWI, df_ACWI, 'DATE')

# Reorder Columns
# get a list of columns
cols = list(df_updated)
# move the column to head of list using index, pop and insert
cols.insert(0, cols.pop(cols.index('DATE')))
cols.insert(1, cols.pop(cols.index('Global')))
cols.insert(2, cols.pop(cols.index('ACWI')))

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

#TODO: Get China Caixin PMI - https://tradingeconomics.com/china/manufacturing-pmi
#TODO: Get China Official PMI - https://tradingeconomics.com/china/business-confidence
#TODO: Get Euro Area EZU
#TODO: Get Euro Area GDP QoQ
#TODO: Get UK EWU

print("Done!")