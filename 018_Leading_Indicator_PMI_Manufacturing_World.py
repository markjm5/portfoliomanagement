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

excel_file_path = '/Trading_Excel_Files/03_Leading_Indicators/018_Temp.xlsm'
sheet_name = 'DB Country PMI'

#get date range
todays_date = date.today()

#use todays date to get pmi month (first day of last month) and use the date in scraping functions
pmi_date = todays_date - relativedelta.relativedelta(months=1)
pmi_date = "01-%s-%s" % (pmi_date.month, pmi_date.year) #make the pmi date the first day of pmi month
pmi_date = dt.strptime(pmi_date, "%d-%m-%Y")
pmi_month = pmi_date.strftime("%B")

def extract_countries_pmi(pmi_date):

    data = {'Date': []}
    
    # Convert the dictionary into DataFrame
    df_countries_pmi = pd.DataFrame(data)

    countries = ['united-states','euro-area','japan','germany','france','united-kingdom','italy','spain','brazil'
                ,'mexico','russia','india','canada','australia','indonesia','south-korea','taiwan','greece','ireland',
                'turkey','czech-republic','poland','denmark','vietnam','thailand','south-africa','hong-kong','saudi-arabia','new-zealand']

    for country in countries:
        #TODO: Scrape The Following:
        #https://tradingeconomics.com/china/business-confidence

        url_pmi = "https://tradingeconomics.com/%s/indicators" % (country)

        page = requests.get(url=url_pmi,verify=False)
        soup = BeautifulSoup(page.content, 'html.parser')

        #Get all html tables on the page
        tables = soup.find_all('table')    
        table_country_pmi = tables[0]

        table_rows = table_country_pmi.find_all('tbody')[0].find_all('tr')
        #table_rows_header = table_country_pmi.find_all('tr')[1].find_all('th')
        df_country_pmi = pd.DataFrame()
        temp_row = []

        #Insert New Row. Format the data to show percentage as float
        for tr in table_rows:
            if(tr.find('td').text.strip() == 'Manufacturing PMI'):
                td = tr.find_all('td')
                for obs in td:
                    text = str(obs.text).strip()
                    temp_row.append(text)        

        import pdb; pdb.set_trace()

        #TODO: Extract Date and PMI
        #TODO: Add Date and PMI to df_countries_pmi

        print(country)

    return df_countries_pmi


#Get Country Rankings
df_countries_pmi = extract_countries_pmi(pmi_date)

# Load original data from excel file into original df
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

import pdb; pdb.set_trace()

#Combine new data with original data
df_updated = combine_df_on_index(df_original, df_countries_pmi, 'Date')

"""
#TODO: Preparation: Load data from each country tab from Cell A5 into dataframe, merge based on date, and write to new excel sheet

for country in countries:

    sheet_name = country

    # Load original data from excel file into original df
    df_country_data = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

    #Combine new data with original data
    df_countries_pmi = combine_df_on_index(df_country_data, df_countries_pmi, 'Date')

    print(country)
"""

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_countries_pmi, False, 0)