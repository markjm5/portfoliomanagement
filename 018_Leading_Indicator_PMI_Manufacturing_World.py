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

def extract_countries_pmi():

    data = {'Date': []}
    
    # Convert the dictionary into DataFrame
    df_countries_pmi = pd.DataFrame(data)

    countries = ['united-states','euro-area','japan','germany','france','united-kingdom','italy','spain','brazil'
                ,'mexico','russia','india','canada','australia','indonesia','south-korea','taiwan','greece','ireland',
                'turkey','czech-republic','poland','denmark','vietnam','thailand','south-africa','hong-kong','saudi-arabia','new-zealand','china']

    for country in countries:
        #TODO: Scrape The Following:
        #https://tradingeconomics.com/china/business-confidence
        #url_pmi = "https://tradingeconomics.com/%s/indicators" % (country)

        data2 = {country: [], "Date": []}
        df_country_pmi = pd.DataFrame(data2)

        df_country_pmi = scrape_table_country_pmi("https://tradingeconomics.com/%s/manufacturing-pmi" % (country,),country)

        #df_country_pmi.loc[len(df_country_pmi.index)] = temp_row

        # Format columns as date and numeric types
        df_country_pmi["Date"] = pd.to_datetime(df_country_pmi["Date"], format='%b-%y')
        df_country_pmi[country] = pd.to_numeric(df_country_pmi[country])

        # Combine Date and PMI with existing df_countries_pmi
        df_countries_pmi = combine_df_on_index(df_countries_pmi, df_country_pmi,'Date')

    return df_countries_pmi


def scrape_table_country_pmi(url, country):

    page = requests.get(url=url)
    soup = BeautifulSoup(page.content, 'html.parser')

    #TODO: Need to scrape table for world production countries and numbers.
    table = soup.find_all('table')

    table_rows = table[1].find_all('tr', recursive=False)

    data = {country: [], "Date": []}
    df = pd.DataFrame(data)
    index = 0

    #Get rows of data.
    for tr in table_rows:
        temp_row = []

        if(tr.find('td').text.strip() == 'Manufacturing PMI'):
            #first_col = True
            index = 0
            td = tr.find_all('td')

            for obs in td:
                text = str(obs.text).strip()
                if(index == 1):
                    temp_row.append(text)        
                if(index == 4):
                    #TODO: Format Date before inserting into temp_row
                    dt_date = dt.strptime(text,'%b/%y')
                    text = dt_date.strftime('%b-%y')
                    temp_row.append(text)
                index += 1

        if(temp_row):
            print("Retrieved Data For: %s - %s" % (country, temp_row))
            df.loc[len(df.index)] = temp_row
    return df

#####################################################
# Get PMI Index for Countries from TradingEconomics #
#####################################################
sheet_name = 'DB Country PMI'

#Get Country Rankings
df_countries_pmi = extract_countries_pmi()

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
df_ism_headline_index = df_ism_headline_index.drop('NEW_ORDERS', 1)
df_ism_headline_index = df_ism_headline_index.drop('IMPORTS', 1)
df_ism_headline_index = df_ism_headline_index.drop('BACKLOG_OF_ORDERS', 1)
df_ism_headline_index = df_ism_headline_index.drop('PRICES', 1)
df_ism_headline_index = df_ism_headline_index.drop('PRODUCTION', 1)
df_ism_headline_index = df_ism_headline_index.drop('CUSTOMERS_INVENTORIES', 1)
df_ism_headline_index = df_ism_headline_index.drop('INVENTORIES', 1)
df_ism_headline_index = df_ism_headline_index.drop('DELIVERIES', 1)
df_ism_headline_index = df_ism_headline_index.drop('EMPLOYMENT', 1)
df_ism_headline_index = df_ism_headline_index.drop('EXPORTS', 1)

import pdb; pdb.set_trace()

#TODO: Get China Caixin PMI - https://tradingeconomics.com/china/manufacturing-pmi
#TODO: Get China Official PMI
#TODO: Get Euro Area EZU
#TODO: Get Euro Area GDP QoQ
#TODO: Get UK EWU

print("Done!")