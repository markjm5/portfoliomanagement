from multiprocessing.sharedctypes import Value
import requests
import calendar
import re
import pandas as pd
import xml.etree.ElementTree as ET
from inspect import getmembers, isclass, isfunction
from datetime import datetime as dt
from dateutil import parser, relativedelta
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from requests.models import parse_header_links
from common import convert_excelsheet_to_dataframe, write_dataframe_to_excel
from common import combine_df_on_index, get_yf_data, get_gdp_fred,get_oecd_data
from common import get_ism_manufacturing_content, scrape_ism_manufacturing_headline_index
from common import get_stlouisfed_data

excel_file_path = '/Trading_Excel_Files/03_Leading_Indicators/018_Leading_Indicator_PMI_Manufacturing_World.xlsm'

def get_current_ffr_target():

    url = "https://www.bankrate.com/rates/interest-rates/federal-funds-rate.aspx"

    page = requests.get(url=url)
    soup = BeautifulSoup(page.content, 'html.parser')
    
    table = soup.find('table')

    ffr_str = table.find('tbody').find('tr').find('td').text.strip()

    pattern_select = re.compile(r'([0-9\.\-]*(?=\)))')

    arr_rate = pattern_select.findall(ffr_str)
    current_ffr_target = ""

    if(len(arr_rate) > 0):
        current_ffr_target = arr_rate[0]

    return current_ffr_target

def get_eurodollar_futures():

    url = "https://www.cmegroup.com/markets/interest-rates/stirs/eurodollar.quotes.html"

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 640 XL LTE) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Mobile Safari/537.36 Edge/12.10166")

    driver = webdriver.Chrome(ChromeDriverManager().install(),options=chrome_options)
    driver.get(url)
    html = driver.page_source
    driver.close()

    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table')

    # Iterate through table elements and get eurodollar futures quotes
    table_rows = table.find_all('tr')
    table_rows_header = table.find_all('tr')[0].find_all('th')

    df = pd.DataFrame()
    index = 0

    for header in table_rows_header:
        df.insert(index,header.text,[],True)
        index+=1

    #Insert New Row. Format the data to show percentage as float
    for tr in table_rows:
        temp_row = []

        td = tr.find_all('td')
        if(td):
            for obs in td:
                text = obs.text
                temp_row.append(text)        

            df.loc[len(df.index)] = temp_row

    df = df.drop(columns=['Change','Options','Chart','Open', 'High', 'Low', 'Volume', 'Updated', 'PriorSettle'], axis=1)
    pattern_select = re.compile(r'((?!=[0-9])GE[A-Z][0-9])')

    df['Month'] = df['Month'].str.replace(pattern_select,'')

    # format date
    df['Month'] = pd.to_datetime(df['Month'],format='%b %Y')

    current_value = None
    one_month_value = None
    six_month_value = None
    twelve_month_value = None

    # Calculate Eurodollar Futures quotes for 1m, 6m, 12m
    for index, row in df.iterrows():
        if(index==0):
            try:
                current_value = float(row['Last'])
            except ValueError as e:
                pass        
        elif(index==2):
            try:
                one_month_value = current_value - float(row['Last'])
            except ValueError as e:
                pass        

        elif(index==5):
            try:
                six_month_value = current_value - float(row['Last'])
            except ValueError as e:
                pass        

        elif(index==11):
            try:
                twelve_month_value = current_value - float(row['Last'])
            except ValueError as e:
                pass        

        print("%s-%s" % (row['Month'], row['Last']))

    # initialize list of lists
    data = [['Euro Futures 1m', one_month_value], ['Euro Futures 6m', six_month_value], ['Euro Futures 12m', twelve_month_value]]
    
    # Create the pandas DataFrame
    df_eurodollar_futures = pd.DataFrame(columns=['COL1', 'LAST'], data=data)

    return df_eurodollar_futures    

############################################
# Get US Lagging and Coincident Indicators #
############################################
"""
sheet_name = 'DB US Lagging Indicators'

#US GDP
df_GDPC1 = get_gdp_fred('GDPC1')
#TODO: Get last GDP Number (QoQ, YoY). Then get GDP numbers for 6m and 12m ago from last

#US  Core CPI
df_CPILFESL = get_stlouisfed_data('CPILFESL')

#US  Core PCE
df_PCEPILFE = get_stlouisfed_data('PCEPILFE')
lyca
#US Retail Sales Ex Auto and Gas
df_MARTSSM44W72USS = get_stlouisfed_data('MARTSSM44W72USS')

#US Unemployment Rate
df_UNRATE = get_stlouisfed_data('UNRATE')

#US NFP
df_PAYEMS = get_stlouisfed_data('PAYEMS')

#US Weekly Claims
df_ICSA = get_stlouisfed_data('ICSA')

#US Industrial Production
df_INDPRO = get_stlouisfed_data('INDPRO')

##################################
# Get US Rates and Currency Data #
##################################
sheet_name = 'DB US Rates and Currency'

# Get Current Fed Funds Current Target Rate
current_ffr_target = get_current_ffr_target()

# Calculate Eurodollar Futures quotes for 1m, 6m, 12m
df_eurodollar_futures = get_eurodollar_futures()

# Get US Treasury Yields #
excel_file_path_013 = '/Trading_Excel_Files/02_Interest_Rates_FX/013_Yield_Curve.xlsm'
sheet_name_013 = 'Database'

# Get Original Sheet and store it in a dataframe
df_data_013 = convert_excelsheet_to_dataframe(excel_file_path_013, sheet_name_013, True)

# retrieve Last, 6m and 12m values for 30y, 10y and 2y yields

df_last = df_data_013.loc[(df_data_013['DATE'] == df_data_013['DATE'].max())].T
df_last = df_last.iloc[1: , :]
df_last.rename(columns={ df_last.columns[0]: "LAST" }, inplace = True)
df_last.reset_index(inplace=True)
df_last = df_last.rename(columns = {'index':'RATE'})

df_6_months_ago = df_data_013.loc[(df_data_013['DATE'] == df_data_013['DATE'].max() - pd.DateOffset(months=6))].T
df_6_months_ago = df_6_months_ago.iloc[1: , :]
df_6_months_ago.rename(columns={ df_6_months_ago.columns[0]: "6M" }, inplace = True)
df_6_months_ago.reset_index(inplace=True)
df_6_months_ago = df_6_months_ago.rename(columns = {'index':'RATE'})

df_12_months_ago = df_data_013.loc[(df_data_013['DATE'] == df_data_013['DATE'].max() - pd.DateOffset(months=12))].T
df_12_months_ago = df_12_months_ago.iloc[1: , :]
df_12_months_ago.rename(columns={ df_12_months_ago.columns[0]: "12M" }, inplace = True)
df_12_months_ago.reset_index(inplace=True)
df_12_months_ago = df_12_months_ago.rename(columns = {'index':'RATE'})

df_us_treasury_yields = combine_df_on_index(df_last, df_6_months_ago, 'RATE')
df_us_treasury_yields = combine_df_on_index(df_us_treasury_yields, df_12_months_ago, 'RATE')

# get a list of columns
cols = list(df_us_treasury_yields)
# move the column to head of list
cols.insert(0, cols.pop(cols.index('RATE')))
cols.insert(1, cols.pop(cols.index('LAST')))
cols.insert(2, cols.pop(cols.index('6M')))
cols.insert(3, cols.pop(cols.index('12M')))

# reorder
df_us_treasury_yields = df_us_treasury_yields[cols]

# Get DXY for Last, 6m, 12m
excel_file_path_001 = '/Trading_Excel_Files/01_Lagging_Coincident_Indicators/001_Lagging_Indicator_YoY_Asset_Class_Performance.xlsm'

sheet_name_001 = 'Database'
# Get Original Sheet and store it in a dataframe
df_data_001 = convert_excelsheet_to_dataframe(excel_file_path_001, sheet_name_001, True)

df_data_001 = df_data_001.filter(['DATE','DX-Y.NYB'])

df_last = df_data_001.loc[(df_data_001['DATE'] == df_data_001['DATE'].max())].T
df_last = df_last.iloc[1: , :]
df_last.rename(columns={ df_last.columns[0]: "LAST" }, inplace = True)
df_last.reset_index(inplace=True)
df_last = df_last.rename(columns = {'index':'SYMBOL'})

df_6_months_ago = df_data_001.loc[(df_data_001['DATE'] == df_data_001['DATE'].max() - pd.DateOffset(months=6))].T
df_6_months_ago = df_6_months_ago.iloc[1: , :]
df_6_months_ago.rename(columns={ df_6_months_ago.columns[0]: "6M" }, inplace = True)
df_6_months_ago.reset_index(inplace=True)
df_6_months_ago = df_6_months_ago.rename(columns = {'index':'SYMBOL'})

df_12_months_ago = df_data_001.loc[(df_data_001['DATE'] == df_data_001['DATE'].max() - pd.DateOffset(months=12))].T
df_12_months_ago = df_12_months_ago.iloc[1: , :]
df_12_months_ago.rename(columns={ df_12_months_ago.columns[0]: "12M" }, inplace = True)
df_12_months_ago.reset_index(inplace=True)
df_12_months_ago = df_12_months_ago.rename(columns = {'index':'SYMBOL'})

df_dxy = combine_df_on_index(df_last, df_6_months_ago, 'SYMBOL')
df_dxy = combine_df_on_index(df_dxy, df_12_months_ago, 'SYMBOL')

# get a list of columns
cols = list(df_dxy)
# move the column to head of list
cols.insert(0, cols.pop(cols.index('SYMBOL')))
cols.insert(1, cols.pop(cols.index('LAST')))
cols.insert(2, cols.pop(cols.index('6M')))
cols.insert(3, cols.pop(cols.index('12M')))

# reorder
df_dxy = df_dxy[cols]
"""
#############################
# Get US Leading Indicators #
#############################

import pdb; pdb.set_trace()
#TODO: UMCSI Index
#TODO: UMCSI Exp
#TODO: Conference Board LEI
#TODO: Building Permits
#TODO: ISM Manufacturing
#TODO: ISM Manuf New Orders
#TODO: ISM Services
#TODO: Money Supply M1
#TODO: Money Supply M2

#TODO: Get ISM Manufacutring Sectors #


###############################
# Get PMI Manufacturing World #
###############################



print("Done!")