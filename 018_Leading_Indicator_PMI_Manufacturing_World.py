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

data = {'Date': []}
 
# Convert the dictionary into DataFrame
df_countries_pmi = pd.DataFrame(data)

sheet_name_original = 'DB Country PMI'

# Load original data from excel file into original df
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name_original, False)

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

    #df_countries_pmi.append(df_updated, ignore_index=True)

    print(country)

import pdb; pdb.set_trace()



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