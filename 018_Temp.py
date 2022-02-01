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

#TODO: Preparation: Load data from each country tab from Cell A5 into dataframe, merge based on date, and write to new excel sheet
excel_file_path = '/Trading_Excel_Files/03_Leading_Indicators/018_Temp.xlsm'
sheet_name_original = 'DB Temp'

df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name_original, False)

#get date range
todays_date = date.today()

#use todays date to get pmi month (first day of last month) and use the date in scraping functions
pmi_date = todays_date - relativedelta.relativedelta(months=1)
pmi_date = "01-%s-%s" % (pmi_date.month, pmi_date.year) #make the pmi date the first day of pmi month
pmi_date = dt.strptime(pmi_date, "%d-%m-%Y")
pmi_month = pmi_date.strftime("%B")

countries = ['US','EuroArea','Japan','Germany','France','UK','Italy','Spain','Brazil'
            ,'Mexico','Russia','India','Canada','Australia','Indonesia','SouthKorea','Taiwan','Greece','Ireland',
            'Turkey','CzechRepublic','Poland','Denmark','Vietnam','Thailand','SouthAfrica','HongKong','SaudiArabia','NewZealand']

for country in countries:

    sheet_name = country

    # Load original data from excel file into original df
    df_country_data = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

    #Combine new data with original data
    df_original = combine_df_on_index(df_country_data, df_original, 'Date')

    print(country)

import pdb; pdb.set_trace()
# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name_original, df_original, False, 0)