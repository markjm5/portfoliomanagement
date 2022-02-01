from json.decoder import JSONDecodeError
import sys
from xml.dom.minidom import Attr
from idna import InvalidCodepointContext
import requests
import os.path
import csv
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
from common import combine_df_on_index, convert_html_table_to_df, _util_check_diff_list, _transform_data

excel_file_path = '/Trading_Excel_Files/03_Leading_Indicators/018_Leading_Indicator_PMI_Manufacturing_World.xlsm'
sheet_name = 'DB Country PMI'

#get date range
todays_date = date.today()

#use todays date to get pmi month (first day of last month) and use the date in scraping functions
pmi_date = todays_date - relativedelta.relativedelta(months=1)
pmi_date = "01-%s-%s" % (pmi_date.month, pmi_date.year) #make the pmi date the first day of pmi month
pmi_date = dt.strptime(pmi_date, "%d-%m-%Y")
pmi_month = pmi_date.strftime("%B")

def extract_countries_pmi():

    data = {'Date': []}
    
    # Convert the dictionary into DataFrame
    df_countries_pmi = pd.DataFrame(data)

    countries = ['united-states','euro-area','japan','germany','france','united-kingdom','italy','spain','brazil'
                ,'mexico','russia','india','canada','australia','indonesia','south-korea','taiwan','greece','ireland',
                'turkey','czech-republic','poland','denmark','vietnam','thailand','south-africa','hong-kong','saudi-arabia','new-zealand']

    for country in countries:
        #TODO: Scrape The Following:
        #https://tradingeconomics.com/china/business-confidence
        #url_pmi = "https://tradingeconomics.com/%s/indicators" % (country)

        data2 = {country: [], "Date": []}
        df_country_pmi = pd.DataFrame(data2)

        #if(not temp_row):
        #For Denmark and Thailand Manufacturing PMI
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


#Get Country Rankings
df_countries_pmi = extract_countries_pmi()

# Load original data from excel file into original df
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

#Combine new data with original data
df_updated = combine_df_on_index(df_original, df_countries_pmi, 'Date')

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)