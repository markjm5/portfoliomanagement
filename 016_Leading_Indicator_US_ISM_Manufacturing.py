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
from common import combine_df_on_index, get_stlouisfed_data, get_yf_data, convert_html_table_to_df

excel_file_path = '/Trading_Excel_Files/03_Leading_Indicators/016_Leading_Indicator_US_ISM_Manufacturing.xlsm'
sheet_name = 'DB Manufacturing ISM'

def scrape_pmi_manufacturing_index():

    #get date range
    todays_date = date.today()

    #use todays date to get pmi month (last month) and use the month string to call the url
    pmi_date = todays_date - relativedelta.relativedelta(months=2)
    pmi_month = pmi_date.strftime("%B")
    url_pmi = 'https://www.ismworld.org/supply-management-news-and-reports/reports/ism-report-on-business/pmi/%s' % (pmi_month.lower(),)

    page = requests.get(url=url_pmi,verify=False)
    soup = BeautifulSoup(page.content, 'html.parser')

    #Get all html tables on the page
    tables = soup.find_all('table')    
    table_at_a_glance = tables[0]
    table_last_12_months_a = tables[1] 
    table_last_12_months_b = tables[2] 
    table_new_orders = tables[3] 
    table_production = tables[4] 
    
    #Convert the tables into dataframes so that we can read the data
    df_at_a_glance = convert_html_table_to_df(table_at_a_glance, True)
    df_last_12_months_a = convert_html_table_to_df(table_last_12_months_a, True)
    df_last_12_months_b = convert_html_table_to_df(table_last_12_months_b, True)
    df_new_orders = convert_html_table_to_df(table_new_orders, True)
    df_production = convert_html_table_to_df(table_production, True)

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

    return df_at_a_glance, df_new_orders, df_production, para_manufacturing, para_new_orders, para_production

def extract_rankings(industry_str):

    #TODO: convert para text into ranking of industries
    substr = industry_str[industry_str.index(': ')+2:industry_str.index('.')]
    arr_substr = substr.replace('and ', '').split(';')

    #TODO: Turn into df with a column for DATE, and columns for each industry. And a single row for the ranking numbers
    # Example - October 2021 - 
    # Apparel, Leather & Allied Products; 16
    # Furniture & Related Products; 15
    # Textile Mills; 14
    # Electrical Equipment, Appliances & Components; 13
    # Machinery; 12
    # Printing & Related Support Activities; 11
    # Food, Beverage & Tobacco Products; 10
    # Computer & Electronic Products; 9
    # Chemical Products; 8
    # Fabricated Metal Products; 7 
    # Miscellaneous Manufacturing; 6 
    # Petroleum & Coal Products; 5 
    # Plastics & Rubber Products; 4
    # Paper Products; 3
    # Primary Metals; 2 
    # Transportation Equipment. 1
    # The two industries reporting a decrease in October compared to September are 
    # Wood Products; -2
    # Nonmetallic Mineral Products. -1

    return arr_substr

df_at_a_glance, df_new_orders, df_production, para_manufacturing, para_new_orders, para_production = scrape_pmi_manufacturing_index()

arr_manufacturing = extract_rankings(para_manufacturing)
arr_new_orders = extract_rankings(para_new_orders)
arr_production = extract_rankings(para_production)

import pdb; pdb.set_trace()

#print(df_at_a_glance.head())
#print(df_new_orders.head())
#print(df_production.head())
#print(para_manufacturing)
#print(para_new_orders)
#print(para_production)

#TODO: Update the the following tabs:
#Sectors Trend
#Details
#ISM Manufacturing vs GDP
#ISM Manufacturing vs SPX
#ISM Manufacturing
#Industry Comments ?

print("Done!")
