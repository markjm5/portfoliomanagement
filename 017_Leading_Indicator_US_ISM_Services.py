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
from common import get_us_gdp_fred, get_sp500_monthly_prices, convert_excelsheet_to_dataframe, write_dataframe_to_excel
from common import combine_df_on_index, convert_html_table_to_df, _util_check_diff_list, _transform_data

excel_file_path = '/Trading_Excel_Files/03_Leading_Indicators/017_Leading_Indicator_US_ISM_Services.xlsm'

#get date range
todays_date = date.today()

#use todays date to get pmi month (first day of last month) and use the date in scraping functions
pmi_date = todays_date - relativedelta.relativedelta(months=1)
pmi_date = "01-%s-%s" % (pmi_date.month, pmi_date.year) #make the pmi date the first day of pmi month
pmi_date = dt.strptime(pmi_date, "%d-%m-%Y")
pmi_month = pmi_date.strftime("%B")

url_pmi = 'https://www.ismworld.org/supply-management-news-and-reports/reports/ism-report-on-business/services/%s' % (pmi_month.lower(),)

def scrape_manufacturing_new_orders_production(pmi_date):

    page = requests.get(url=url_pmi,verify=False)
    soup = BeautifulSoup(page.content, 'html.parser')

    paras = soup.find_all("p")

    para_services = "" 
    para_new_orders = ""
    para_business = ""
    pattern_select = re.compile(r'((?<=following order:\s)[A-Za-z,&;\s]*.|(?<=are:\s)[A-Za-z,&;\s]*.)')

    for para in paras:
        #Get the specific paragraph
        if('services industries' in para.text and '%s' % (pmi_month) in para.text and len(pattern_select.findall(para.text)) > 0):
            para_services = para.text

        if('new orders' in para.text and '%s' % (pmi_month) in para.text and len(pattern_select.findall(para.text)) > 0):
            para_new_orders = para.text

        if('business activity' in para.text and '%s' % (pmi_month) in para.text and len(pattern_select.findall(para.text)) > 0):
            para_business = para.text

    return para_services, para_new_orders, para_business


def scrape_pmi_headline_index(pmi_date):

    page = requests.get(url=url_pmi,verify=False)
    soup = BeautifulSoup(page.content, 'html.parser')

    #Get all html tables on the page
    tables = soup.find_all('table')    
    table_at_a_glance = tables[0]
    
    #Convert the tables into dataframes so that we can read the data
    #df_at_a_glance = convert_html_table_to_df(table_at_a_glance, True)

    table_rows = table_at_a_glance.find_all('tbody')[0].find_all('tr')
    table_rows_header = table_at_a_glance.find_all('tr')[1].find_all('th')
    df_at_a_glance = pd.DataFrame()

    index = 0

    for header in table_rows_header:
        df_at_a_glance.insert(index,str(header.text).strip(),[],True)
        index+=1

    #Insert New Row. Format the data to show percentage as float
    for tr in table_rows:
        temp_row = []

        tr_th = tr.find('th')
        text = str(tr_th.text).strip()
        temp_row.append(text)        

        td = tr.find_all('td')
        for obs in td:
            text = str(obs.text).strip()
            temp_row.append(text)        
        
        if(len(temp_row) == len(df_at_a_glance.columns)):
            df_at_a_glance.loc[len(df_at_a_glance.index)] = temp_row
    
    #Drop Unnecessary Columns
    column_numbers = [x for x in range(df_at_a_glance.shape[1])]  # list of columns' integer indices
    column_numbers .remove(7)
    column_numbers .remove(8)
    column_numbers .remove(9)

    df_at_a_glance = df_at_a_glance.iloc[:, column_numbers] #return all columns except the 0th column

    #Flip df around
    df_at_a_glance = df_at_a_glance.T

    # Rename Columns as per requirements of excel file 017
    df_at_a_glance = df_at_a_glance.rename(columns={0: "ISM_SERVICES", 1:"BUSINESS_ACTIVITY",2:"NEW_ORDERS",3:"EMPLOYMENT",4:"DELIVERIES",
                                                    5:"INVENTORIES",6:"PRICES",7:"BACKLOG_OF_ORDERS",8:"EXPORTS",9:"IMPORTS",10:"INVENTORY_SENTIMENT",11:"CUSTOMER_INVENTORIES"})

    #Drop the first row because it contains the old column names
    df_at_a_glance = df_at_a_glance.iloc[1: , :]
    df_at_a_glance = df_at_a_glance.head(1)
    df_at_a_glance = df_at_a_glance.reset_index()
    df_at_a_glance = df_at_a_glance.drop(columns='index', axis=1)
    df_at_a_glance = df_at_a_glance.drop(columns='CUSTOMER_INVENTORIES', axis=1)

    #Fix datatypes of df_at_a_glance
    for column in df_at_a_glance:
        df_at_a_glance[column] = pd.to_numeric(df_at_a_glance[column])

    #Add DATE column to df
    df_at_a_glance["DATE"] = [pmi_date]

    # Put DATE as the first column
    # get a list of columns
    cols = list(df_at_a_glance)
    cols.insert(0, cols.pop(cols.index('DATE')))

    # reorder
    df_at_a_glance = df_at_a_glance[cols]

    return df_at_a_glance

def scrape_industry_comments(pmi_date):

    page = requests.get(url=url_pmi,verify=False)
    soup = BeautifulSoup(page.content, 'html.parser')

    #Get all html tables on the page
    lis = soup.find_all('li')   
    arr_comments = []

    #The regex pattern for industry comments
    pattern = re.compile("“[()’A-Za-z,&;\s\.0-9\-]*”\s(\[)[A-Za-z,&;\s]*(\])")

    for li in lis:
        matched = pattern.match(li.text)
        if(matched):
            arr_comments.append(li.text)

    return arr_comments

def return_df_comments(arr_comments, pmi_date):
    df_comments = pd.DataFrame(columns=['Date','Sector','Comments'])

    #TODO: Use regex to extract comment and industry name. Regex Cheat Sheet: https://www.rexegg.com/regex-quickstart.html
    pattern_comment = re.compile(r'“[()’A-Za-z,&;\s\.0-9\-]*”')
    pattern_industry = re.compile(r'((?<=\[)[A-Za-z,&;\s]*(?<!\]))')

    for comment in arr_comments:
        matches_comment = re.search(pattern_comment,comment).group(0)
        matches_industry = re.search(pattern_industry,comment).group(0)
        df_comments = df_comments.append({'Date': pmi_date, 'Sector': matches_industry, 'Comments': matches_comment}, ignore_index=True)

    return df_comments

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
    try:
        increase_arr = match_arr[0].split(';')
    except IndexError as e:
        increase_arr = []
    try:
        decrease_arr = match_arr[1].split(';')        
    except IndexError as e:
        decrease_arr = []

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
        df_columns_18_industries = ['Utilities','Retail Trade','Arts, Entertainment & Recreation','Other Services','Health Care & Social Assistance','Accommodation & Food Services',
                                    'Transportation & Warehousing','Finance & Insurance','Real Estate, Rental & Leasing','Public Administration','Agriculture, Forestry, Fishing & Hunting',
                                    'Construction','Professional, Scientific & Technical Services','Wholesale Trade','Management of Companies & Support Services','Mining',
                                    'Information','Educational Services']

        #Find out what columns are missing
        missing_columns = _util_check_diff_list(df_columns_18_industries,df_rankings.columns)
        
        #Add missing columns to df_ranking with zero as the rank number
        for col in missing_columns:
            df_rankings[col] = [0]

    #Add DATE column to df
    df_rankings["DATE"] = [pmi_date]

    return df_rankings


#df_at_a_glance, df_new_orders, df_production, para_manufacturing, para_new_orders, para_production = scrape_pmi_manufacturing_index(pmi_date)
para_services, para_new_orders, para_business = scrape_manufacturing_new_orders_production(pmi_date)

#############################
# Get Services ISM Rankings #
#############################

sheet_name = 'DB Services ISM'

#_transform_data(excel_file_path, sheet_name, sheet_name)

#Get rankings
df_services_rankings = extract_rankings(para_services,pmi_date)

# Load original data from excel file into original df
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

#Combine new data with original data
df_updated = combine_df_on_index(df_original, df_services_rankings, 'DATE')

# Reorder Columns
# get a list of columns
cols = list(df_updated)
# move the column to head of list using index, pop and insert
cols.insert(0, cols.pop(cols.index('DATE')))
cols.insert(1, cols.pop(cols.index('Arts, Entertainment & Recreation')))
cols.insert(2, cols.pop(cols.index('Other Services')))
cols.insert(3, cols.pop(cols.index('Health Care & Social Assistance')))
cols.insert(4, cols.pop(cols.index('Accommodation & Food Services')))
cols.insert(5, cols.pop(cols.index('Finance & Insurance')))
cols.insert(6, cols.pop(cols.index('Real Estate, Rental & Leasing')))
cols.insert(7, cols.pop(cols.index('Transportation & Warehousing')))
cols.insert(8, cols.pop(cols.index('Mining')))
cols.insert(9, cols.pop(cols.index('Construction')))
cols.insert(10, cols.pop(cols.index('Wholesale Trade')))
cols.insert(11, cols.pop(cols.index('Public Administration')))
cols.insert(12, cols.pop(cols.index('Professional, Scientific & Technical Services')))
cols.insert(13, cols.pop(cols.index('Agriculture, Forestry, Fishing & Hunting')))
cols.insert(14, cols.pop(cols.index('Information')))
cols.insert(15, cols.pop(cols.index('Educational Services')))
cols.insert(16, cols.pop(cols.index('Management of Companies & Support Services')))

# reorder
df_updated = df_updated[cols]

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

###########################
# Get Business Rankings #
###########################

sheet_name = 'DB Business'
#_transform_data(excel_file_path, sheet_name, sheet_name)

#Get rankings
df_business_rankings = extract_rankings(para_business,pmi_date)

# Load original data from excel file into original df
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

#Combine new data with original data
df_updated = combine_df_on_index(df_original, df_business_rankings, 'DATE')

# Reorder Columns
# get a list of columns
cols = list(df_updated)
# move the column to head of list using index, pop and insert
cols.insert(0, cols.pop(cols.index('DATE')))
cols.insert(1, cols.pop(cols.index('Arts, Entertainment & Recreation')))
cols.insert(2, cols.pop(cols.index('Other Services')))
cols.insert(3, cols.pop(cols.index('Health Care & Social Assistance')))
cols.insert(4, cols.pop(cols.index('Accommodation & Food Services')))
cols.insert(5, cols.pop(cols.index('Finance & Insurance')))
cols.insert(6, cols.pop(cols.index('Real Estate, Rental & Leasing')))
cols.insert(7, cols.pop(cols.index('Transportation & Warehousing')))
cols.insert(8, cols.pop(cols.index('Mining')))
cols.insert(9, cols.pop(cols.index('Construction')))
cols.insert(10, cols.pop(cols.index('Wholesale Trade')))
cols.insert(11, cols.pop(cols.index('Public Administration')))
cols.insert(12, cols.pop(cols.index('Professional, Scientific & Technical Services')))
cols.insert(13, cols.pop(cols.index('Agriculture, Forestry, Fishing & Hunting')))
cols.insert(14, cols.pop(cols.index('Information')))
cols.insert(15, cols.pop(cols.index('Educational Services')))
cols.insert(16, cols.pop(cols.index('Management of Companies & Support Services')))

# reorder
df_updated = df_updated[cols]

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

###########################
# Get New Orders Rankings #
###########################

sheet_name = 'DB New Orders'
#_transform_data(excel_file_path, sheet_name, sheet_name)

#Get rankings
df_new_orders_rankings = extract_rankings(para_new_orders,pmi_date)

# Load original data from excel file into original df
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

#Combine new data with original data
df_updated = combine_df_on_index(df_original, df_new_orders_rankings, 'DATE')

# Reorder Columns
# get a list of columns
cols = list(df_updated)
# move the column to head of list using index, pop and insert
cols.insert(0, cols.pop(cols.index('DATE')))
cols.insert(1, cols.pop(cols.index('Arts, Entertainment & Recreation')))
cols.insert(2, cols.pop(cols.index('Other Services')))
cols.insert(3, cols.pop(cols.index('Health Care & Social Assistance')))
cols.insert(4, cols.pop(cols.index('Accommodation & Food Services')))
cols.insert(5, cols.pop(cols.index('Finance & Insurance')))
cols.insert(6, cols.pop(cols.index('Real Estate, Rental & Leasing')))
cols.insert(7, cols.pop(cols.index('Transportation & Warehousing')))
cols.insert(8, cols.pop(cols.index('Mining')))
cols.insert(9, cols.pop(cols.index('Construction')))
cols.insert(10, cols.pop(cols.index('Wholesale Trade')))
cols.insert(11, cols.pop(cols.index('Public Administration')))
cols.insert(12, cols.pop(cols.index('Professional, Scientific & Technical Services')))
cols.insert(13, cols.pop(cols.index('Agriculture, Forestry, Fishing & Hunting')))
cols.insert(14, cols.pop(cols.index('Information')))
cols.insert(15, cols.pop(cols.index('Educational Services')))
cols.insert(16, cols.pop(cols.index('Management of Companies & Support Services')))

# reorder
df_updated = df_updated[cols]

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
cols.insert(5, cols.pop(cols.index('BUSINESS_ACTIVITY')))
cols.insert(6, cols.pop(cols.index('INVENTORY_SENTIMENT')))
cols.insert(7, cols.pop(cols.index('INVENTORIES')))
cols.insert(8, cols.pop(cols.index('DELIVERIES')))
cols.insert(9, cols.pop(cols.index('EMPLOYMENT')))
cols.insert(10, cols.pop(cols.index('EXPORTS')))
cols.insert(11, cols.pop(cols.index('ISM_SERVICES')))
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

############################
# Get Respondents Comments #
############################

sheet_name = 'Industry Comments'

# Scrape 'What Respondents Are Saying' comments:
arr_comments = scrape_industry_comments(pmi_date)
df_comments = return_df_comments(arr_comments, pmi_date)

# Load original data from excel file into original df
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

# Append to df_original with new comments
df_updated = df_original.append(df_comments, ignore_index=True)

# Order by Sector in Ascending Order, then by Date in Decending Order
df_updated = df_updated.sort_values(by=['Sector','Date'], ascending=(True,False))
df_updated = df_updated.reset_index(drop=True)

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 1)

print("Done!")
