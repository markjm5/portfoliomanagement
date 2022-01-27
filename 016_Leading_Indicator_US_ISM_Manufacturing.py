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
from common import get_us_gdp_fred, get_sp500_monthly_prices, convert_excelsheet_to_dataframe, write_dataframe_to_excel
from common import combine_df_on_index, convert_html_table_to_df, _util_check_diff_list

excel_file_path = '/Trading_Excel_Files/03_Leading_Indicators/016_Leading_Indicator_US_ISM_Manufacturing.xlsm'

def scrape_manufacturing_new_orders_production(pmi_date):

    pmi_month = pmi_date.strftime("%B")
    url_pmi = 'https://www.ismworld.org/supply-management-news-and-reports/reports/ism-report-on-business/pmi/%s' % (pmi_month.lower(),)

    page = requests.get(url=url_pmi,verify=False)
    soup = BeautifulSoup(page.content, 'html.parser')

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

    return para_manufacturing, para_new_orders, para_production


def scrape_pmi_headline_index(pmi_date):

    pmi_month = pmi_date.strftime("%B")
    url_pmi = 'https://www.ismworld.org/supply-management-news-and-reports/reports/ism-report-on-business/pmi/%s' % (pmi_month.lower(),)

    page = requests.get(url=url_pmi,verify=False)
    soup = BeautifulSoup(page.content, 'html.parser')

    #Get all html tables on the page
    tables = soup.find_all('table')    
    table_at_a_glance = tables[0]
    
    #Convert the tables into dataframes so that we can read the data
    df_at_a_glance = convert_html_table_to_df(table_at_a_glance, True)

    #Drop Unnecessary Columns
    column_numbers = [x for x in range(df_at_a_glance.shape[1])]  # list of columns' integer indices
    column_numbers .remove(2) #removing column integer index 0
    column_numbers .remove(3)
    column_numbers .remove(4)
    column_numbers .remove(5)
    column_numbers .remove(6)
    df_at_a_glance = df_at_a_glance.iloc[:, column_numbers] #return all columns except the 0th column

    #Flip df around
    df_at_a_glance = df_at_a_glance.T

    #Rename Columns
    df_at_a_glance = df_at_a_glance.rename(columns={0: "ISM", 1:"NEW_ORDERS",2:"PRODUCTION",3:"EMPLOYMENT",4:"DELIVERIES",
                                                    5:"INVENTORIES",6:"CUSTOMERS_INVENTORIES",7:"PRICES",8:"BACKLOG_OF_ORDERS",9:"EXPORTS",10:"IMPORTS"})

    #Drop the first row because it contains the old column names
    df_at_a_glance = df_at_a_glance.iloc[1: , :]
    df_at_a_glance = df_at_a_glance.reset_index()
    df_at_a_glance = df_at_a_glance.drop('index', 1)

    #Fix datatypes of df_at_a_glance
    for column in df_at_a_glance:
        df_at_a_glance[column] = pd.to_numeric(df_at_a_glance[column])

    #Add DATE column to df
    df_at_a_glance["DATE"] = [pmi_date]

    # Reorder Columns
    # get a list of columns
    cols = list(df_at_a_glance)
    cols.insert(0, cols.pop(cols.index('DATE')))
    cols.insert(1, cols.pop(cols.index('NEW_ORDERS')))
    cols.insert(2, cols.pop(cols.index('IMPORTS')))
    cols.insert(3, cols.pop(cols.index('BACKLOG_OF_ORDERS')))
    cols.insert(4, cols.pop(cols.index('PRICES')))
    cols.insert(5, cols.pop(cols.index('PRODUCTION')))
    cols.insert(6, cols.pop(cols.index('CUSTOMERS_INVENTORIES')))
    cols.insert(7, cols.pop(cols.index('INVENTORIES')))
    cols.insert(8, cols.pop(cols.index('DELIVERIES')))
    cols.insert(9, cols.pop(cols.index('EMPLOYMENT')))
    cols.insert(10, cols.pop(cols.index('EXPORTS')))
    cols.insert(11, cols.pop(cols.index('ISM')))

    # reorder
    df_at_a_glance = df_at_a_glance[cols]

    return df_at_a_glance


def extract_rankings(industry_str,pmi_date):

    #Use regex (https://pythex.org/) to get substring that contains order of industries. It should return 2 matches - for increase and decrease   
    pattern_select = re.compile(r'((?<=following order:\s)[A-Za-z,&;\s]*.|(?<=are:\s)[A-Za-z,&;\s]*.|(?<=are\s)[A-Za-z,&;\s]*.)')
    matches = pattern_select.finditer(industry_str)
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

        #Find out what columns are missing
        missing_columns = _util_check_diff_list(df_columns_18_industries,df_rankings.columns)
        
        #Add missing columns to df_ranking with zero as the rank number
        for col in missing_columns:
            df_rankings[col] = [0]

    #Add DATE column to df
    df_rankings["DATE"] = [pmi_date]

    # Reorder Columns
    # get a list of columns
    cols = list(df_rankings)
    # move the column to head of list using index, pop and insert
    cols.insert(0, cols.pop(cols.index('DATE')))
    cols.insert(1, cols.pop(cols.index('Apparel, Leather & Allied Products')))
    cols.insert(2, cols.pop(cols.index('Machinery')))
    cols.insert(3, cols.pop(cols.index('Paper Products')))
    cols.insert(4, cols.pop(cols.index('Computer & Electronic Products')))
    cols.insert(5, cols.pop(cols.index('Petroleum & Coal Products')))
    cols.insert(6, cols.pop(cols.index('Primary Metals')))
    cols.insert(7, cols.pop(cols.index('Printing & Related Support Activities')))
    cols.insert(8, cols.pop(cols.index('Furniture & Related Products')))
    cols.insert(9, cols.pop(cols.index('Transportation Equipment')))
    cols.insert(10, cols.pop(cols.index('Chemical Products')))
    cols.insert(11, cols.pop(cols.index('Food, Beverage & Tobacco Products')))
    cols.insert(12, cols.pop(cols.index('Miscellaneous Manufacturing')))
    cols.insert(13, cols.pop(cols.index('Electrical Equipment, Appliances & Components')))
    cols.insert(14, cols.pop(cols.index('Plastics & Rubber Products')))
    cols.insert(15, cols.pop(cols.index('Fabricated Metal Products')))
    cols.insert(16, cols.pop(cols.index('Wood Products')))
    cols.insert(17, cols.pop(cols.index('Textile Mills')))
    cols.insert(18, cols.pop(cols.index('Nonmetallic Mineral Products')))

    # reorder
    df_rankings = df_rankings[cols]

    return df_rankings

#get date range
todays_date = date.today()

#use todays date to get pmi month (first day of last month) and use the date in scraping functions
pmi_date = todays_date - relativedelta.relativedelta(months=1)
pmi_date = "01-%s-%s" % (pmi_date.month, pmi_date.year) #make the pmi date the first day of pmi month
pmi_date = dt.strptime(pmi_date, "%d-%m-%Y")
"""
#df_at_a_glance, df_new_orders, df_production, para_manufacturing, para_new_orders, para_production = scrape_pmi_manufacturing_index(pmi_date)
para_manufacturing, para_new_orders, para_production = scrape_manufacturing_new_orders_production(pmi_date)

##################################
# Get Manufacturing ISM Rankings #
##################################

sheet_name = 'DB Manufacturing ISM'

#Get rankings
df_manufacturing_rankings = extract_rankings(para_manufacturing,pmi_date)

# Load original data from excel file into original df
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

#Combine new data with original data
df_updated = combine_df_on_index(df_original, df_manufacturing_rankings, 'DATE')

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

###########################
# Get New Orders Rankings #
###########################

sheet_name = 'DB New Orders'

#Get rankings
df_new_orders_rankings = extract_rankings(para_new_orders,pmi_date)

# Load original data from excel file into original df
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

#Combine new data with original data
df_updated = combine_df_on_index(df_original, df_new_orders_rankings, 'DATE')

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

###########################
# Get Production Rankings #
###########################

sheet_name = 'DB Production'

#Get rankings
df_production_rankings = extract_rankings(para_production,pmi_date)

# Load original data from excel file into original df
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

#Combine new data with original data
df_updated = combine_df_on_index(df_original, df_production_rankings, 'DATE')

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

#################################################
# Update Details Tab Using ISM Headline Numbers #
#################################################

sheet_name = 'DB Details'

df_pmi_headline_index = scrape_pmi_headline_index(pmi_date)

#################################
# Get US GDP from St Louis FRED #
#################################

#Get US GDP
df_GDPC1 = get_us_gdp_fred()

###########################################
# Get S&P500 Monthly Close Prices from YF #
###########################################

df_SP500 = get_sp500_monthly_prices()

# Combine df_LEI, df_GDPC1, df_SP500 and df_UMCSI into original df
df = combine_df_on_index(df_pmi_headline_index, df_GDPC1, 'DATE')
df = combine_df_on_index(df_SP500, df, 'DATE')

# Load original data from excel file into original df
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

#Combine new data with original data
df_updated = combine_df_on_index(df_original, df, 'DATE')

# get a list of columns
cols = list(df_updated)
# move the column to head of list
cols.insert(0, cols.pop(cols.index('DATE')))
cols.insert(1, cols.pop(cols.index('NEW_ORDERS')))
cols.insert(2, cols.pop(cols.index('IMPORTS')))
cols.insert(3, cols.pop(cols.index('BACKLOG_OF_ORDERS')))
cols.insert(4, cols.pop(cols.index('PRICES')))
cols.insert(5, cols.pop(cols.index('PRODUCTION')))
cols.insert(6, cols.pop(cols.index('CUSTOMERS_INVENTORIES')))
cols.insert(7, cols.pop(cols.index('INVENTORIES')))
cols.insert(8, cols.pop(cols.index('DELIVERIES')))
cols.insert(9, cols.pop(cols.index('EMPLOYMENT')))
cols.insert(10, cols.pop(cols.index('EXPORTS')))
cols.insert(11, cols.pop(cols.index('ISM')))
cols.insert(12, cols.pop(cols.index('SP500')))
cols.insert(13, cols.pop(cols.index('GDPC1')))
cols.insert(14, cols.pop(cols.index('GDPQoQ')))
cols.insert(15, cols.pop(cols.index('GDPYoY')))
cols.insert(16, cols.pop(cols.index('GDPQoQ_ANNUALIZED')))

# reorder
df_updated = df_updated[cols]

#Fill in blank values with previous
df_updated['GDPYoY'].fillna(method='ffill', inplace=True)
df_updated['GDPQoQ'].fillna(method='ffill', inplace=True)
df_updated['GDPQoQ_ANNUALIZED'].fillna(method='ffill', inplace=True)

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)
"""

############################
# Get Respondents Comments #
############################

sheet_name = 'Industry Comments New'

# Load original data from excel file into original df
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

#Fill in blank values with previous
df_original['Sector'].fillna(method='ffill', inplace=True)

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_original, False, 1)

print("Done!")
