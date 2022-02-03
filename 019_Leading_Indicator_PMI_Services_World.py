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
from common import combine_df_on_index

excel_file_path = '/Trading_Excel_Files/03_Leading_Indicators/019_Leading_Indicator_PMI_Services_World.xlsm'

def scrape_table_country_pmi():

    url = "https://tradingeconomics.com/country-list/services-pmi"

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

print("Done!")