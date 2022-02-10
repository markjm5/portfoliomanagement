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

    #TODO: Calculate Eurodollar Futures quotes for 1m, 6m, 12m

    import pdb; pdb.set_trace()
    return df    


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
"""
##################################
# Get US Rates and Currency Data #
##################################
sheet_name = 'DB US Rates and Currency'

#Get Current Fed Funds Current Target Rate

current_ffr_target = get_current_ffr_target()

#TODO: Calculate Eurodollar Futures quotes for 1m, 6m, 12m
# https://www.cmegroup.com/markets/interest-rates/stirs/eurodollar.quotes.html

df_eurodollar_futures = get_eurodollar_futures()

import pdb; pdb.set_trace()

#TODO: Get Bond Yiends for 30y, 10y, 2y, 3m, and yield curve (ie. 10y - 2y)

#TODO get DXY for Last, 6m, 12m

#############################
# Get US Leading Indicators #
#############################





print("Done!")