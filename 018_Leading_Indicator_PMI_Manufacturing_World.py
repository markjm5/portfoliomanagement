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

#url_pmi = 'https://www.ismworld.org/supply-management-news-and-reports/reports/ism-report-on-business/services/%s' % (pmi_month.lower(),)

def extract_countries_pmi(pmi_date):

    data = {'Date': []}
    
    # Convert the dictionary into DataFrame
    df_countries_pmi = pd.DataFrame(data)

    countries = ['Global','US','EuroArea','Japan','Germany','France','UK','Italy','Spain','Brazil'
                ,'Mexico','Russia','India','Canada','Australia','Indonesia','SouthKorea','Taiwan','Greece','Ireland',
                'Turkey','CzechRepublic','Poland','Denmark','Vietnam','Thailand','SouthAfrica','HongKong','SaudiArabia','NewZealand']

    for country in countries:
        url_pmi = "https://tradingeconomics.com/%s/business-confidence" % (country)

        page = requests.get(url=url_pmi,verify=False)

        #TODO: Extract Date and PMI
        #TODO: Add Date and PMI to df_countries_pmi


        print(country)


    #TODO: Scrape The FOllowing:
    #https://tradingeconomics.com/china/business-confidence
    #https://tradingeconomics.com/australia/indicators
    #https://tradingeconomics.com/china/indicators
    #https://tradingeconomics.com/japan/indicators
    #https://tradingeconomics.com/germany/indicators
    #https://tradingeconomics.com/france/indicators
    #https://tradingeconomics.com/united-kingdom/indicators
    #https://tradingeconomics.com/italy/indicators
    #https://tradingeconomics.com/spain/indicators
    #https://tradingeconomics.com/brazil/indicators
    #https://tradingeconomics.com/mexico/indicators
    #https://tradingeconomics.com/russia/indicators
    #https://tradingeconomics.com/india/indicators
    #https://tradingeconomics.com/canada/indicators
    #https://tradingeconomics.com/indonesia/indicators
    #https://tradingeconomics.com/south-korea/indicators
    #https://tradingeconomics.com/taiwan/indicators
    #https://tradingeconomics.com/greece/indicators
    #https://tradingeconomics.com/ireland/indicators
    #https://tradingeconomics.com/turkey/indicators
    #https://tradingeconomics.com/czech-republic/indicators
    #https://tradingeconomics.com/poland/indicators
    #https://tradingeconomics.com/denmark/indicators


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
countries = ['Global','US','EuroArea','Japan','Germany','France','UK','Italy','Spain','Brazil'
                ,'Mexico','Russia','India','Canada','Australia','Indonesia','SouthKorea','Taiwan','Greece','Ireland',
                'Turkey','CzechRepublic','Poland','Denmark','Vietnam','Thailand','SouthAfrica','HongKong','SaudiArabia','NewZealand']

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