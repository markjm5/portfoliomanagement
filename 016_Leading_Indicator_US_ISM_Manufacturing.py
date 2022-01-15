from json.decoder import JSONDecodeError
import sys
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
from common import get_oecd_data, get_invest_data, convert_excelsheet_to_dataframe, write_dataframe_to_excel, combine_df, util_check_diff_list
from common import combine_df_on_index, get_stlouisfed_data, get_yf_data

excel_file_path = '/Trading_Excel_Files/03_Leading_Indicators/016_Leading_Indicator_US_ISM_Manufacturing.xlsm'
sheet_name = 'DB Manufacturing ISM'

def scrape_pmi_manufacturing_index():

    #get date range
    todays_date = date.today()

    #use todays date to get pmi month (last month) and use the month string to call the url
    pmi_date = todays_date - relativedelta.relativedelta(months=1)
    pmi_month = pmi_date.strftime("%B")
    url_pmi = 'https://www.ismworld.org/supply-management-news-and-reports/reports/ism-report-on-business/pmi/%s' % (pmi_month.lower(),)

    page = requests.get(url=url_pmi,verify=False)
    soup = BeautifulSoup(page.content, 'html.parser')

    #TODO: Can we be more specific about which tables to get, rather than getting ALL tables?
    tables = soup.find_all('table')
    
    table_manufacturing_at_a_glance = tables[0]
    table_last_12_months_a = tables[1] 
    table_last_12_months_b = tables[2] 
    table_new_orders = tables[3] 
    table_production = tables[4] 

    #get all paragraphs
    paras = soup.find_all("p", attrs={'class': None})

    para_manufacturing = "" 
    para_new_orders = ""
    para_production = ""

    for para in paras:
        #Get the specific paragraph
        if('manufacturing industries reporting growth in %s' % (pmi_month) in para.text):
            para_manufacturing = para.text

        if('growth in new orders' in para.text and '%s' % (pmi_month) in para.text):
            para_new_orders = para.text

        if('growth in production' in para.text and '%s' % (pmi_month) in para.text):
            para_production = para.text

    import pdb; pdb.set_trace()

    return df_manufacturing, df_new_orders, df_production


df_manufacturing, df_new_orders, df_production = scrape_pmi_manufacturing_index()

#TODO: Get ISM data for the following tabs:
#Sectors Trend
#Details
#ISM Manufacturing vs GDP
#ISM Manufacturing vs SPX
#ISM Manufacturing
#Industry Comments ?

print("Done!")
