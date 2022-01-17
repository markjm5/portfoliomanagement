from json.decoder import JSONDecodeError
import sys
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
from common import get_oecd_data, get_invest_data, convert_excelsheet_to_dataframe, write_dataframe_to_excel, combine_df
from common import combine_df_on_index, get_stlouisfed_data, get_yf_data, convert_html_table_to_df, _util_check_diff_list

excel_file_path = '/Trading_Excel_Files/03_Leading_Indicators/016_Leading_Indicator_US_ISM_Manufacturing.xlsm'
sheet_name = 'DB Manufacturing ISM'

def scrape_pmi_manufacturing_index(pmi_date):

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

def extract_rankings(industry_str,pmi_date):

    #Use regex (https://pythex.org/) to get substring that contains order of industries. It should return 2 matches - for increase and decrease   
    pattern = re.compile(r'((?<=following order:\s)[A-Za-z,&;\s]*.|(?<=are:\s)[A-Za-z,&;\s]*.|(?<=are\s)[A-Za-z,&;\s]*.)')
    matches = pattern.finditer(industry_str)
    match_arr = []
    pattern_remove = r'and|\.'
    for match in matches:
        new_str = re.sub(pattern_remove, '',match.group(0))
        match_arr.append(new_str)

    #put increase and decrease items into arrays
    increase_arr = match_arr[0].split(';')
    decrease_arr = match_arr[1].split(';')        

    df_rankings = pd.DataFrame()

    #Add Rankings columns to df
    ranking = len(increase_arr)
    index = 0
    for industry in increase_arr:
        df_rankings[industry.lstrip()] = [ranking - index]      
        index += 1

    ranking = len(decrease_arr)
    index = 0
    for industry in decrease_arr:
        df_rankings[industry.lstrip()] = [0 - (ranking - index)]      
        index += 1

    if(len(df_rankings.columns) < 18):
        df_columns_18_industries = ['Machinery','Computer & Electronic Products','Paper Products','Apparel, Leather & Allied Products','Printing & Related Support Activities',
                            'Primary Metals','Nonmetallic Mineral Products','Petroleum & Coal Products','Plastics & Rubber Products','Miscellaneous Manufacturing',
                            'Food, Beverage & Tobacco Products','Furniture & Related Products','Transportation Equipment','Chemical Products','Fabricated Metal Products',
                            'Electrical Equipment, Appliances & Components','Textile Mills','Wood Products']

        #TODO: find out what columns are missing, and figure out a way to add numbers
        missing_columns = _util_check_diff_list(df_columns_18_industries,df_rankings.columns)

        import pdb; pdb.set_trace()

    #Add DATE column to df
    df_rankings["DATE"] = [pmi_date]

    #TODO: Reorder Columns

    print(df_rankings.head())

    #TODO: Turn into df with a column for DATE, and columns for each industry. And a single row for the ranking numbers
    # Algorithm should reverse order and assign ranking from 1 onwards for increase. Need to reverse order and assign ranking from -1 onwards for decrease.
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

    return df_rankings


#get date range
todays_date = date.today()

#use todays date to get pmi month (first day of last month) and use the date in scraping functions
pmi_date = todays_date - relativedelta.relativedelta(months=3)
pmi_date = "01-%s-%s" % (pmi_date.month, pmi_date.year) #make the pmi date the first day of pmi month
pmi_date = dt.strptime(pmi_date, "%d-%m-%Y")

df_at_a_glance, df_new_orders, df_production, para_manufacturing, para_new_orders, para_production = scrape_pmi_manufacturing_index(pmi_date)

df_manufacturing_rankings = extract_rankings(para_manufacturing,pmi_date)
import pdb; pdb.set_trace()
df_new_orders_rankings = extract_rankings(para_new_orders,pmi_date)
df_production_rankings = extract_rankings(para_production,pmi_date)

#import pdb;pdb.set_trace()
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
