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
from common import combine_df_on_index, get_yf_data, get_oecd_data
from common import get_ism_manufacturing_content, scrape_ism_manufacturing_headline_index
from common import get_stlouisfed_data, get_data_fred

excel_file_path = '/Trading_Excel_Files/03_Leading_Indicators/022_Global_Macro_Data_Summary_Page.xlsm'

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

    # put value into a df with COL0, LAST headers
    current_ffr_target = "%s%%" % (current_ffr_target)
    data = [['Fed Funds Target Rate', current_ffr_target]]
    
    # Create the pandas DataFrame
    df_current_ffr_target = pd.DataFrame(columns=['COL0', 'LAST'], data=data)

    return df_current_ffr_target

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
    df_eurodollar_futures = pd.DataFrame(columns=['COL0', 'LAST'], data=data)

    return df_eurodollar_futures    

def get_data(df):
    # retrieve Last, 6m and 12m values for data specified in df
    df_last = get_df_row(df, df['DATE'].max(), 'LAST')
    df_6_months_ago = get_df_row(df, df['DATE'].max() - pd.DateOffset(months=6), '6M')
    df_12_months_ago = get_df_row(df, df['DATE'].max() - pd.DateOffset(months=12),'12M')

    df_new = combine_df_on_index(df_last, df_6_months_ago, 'COL0')
    df_new = combine_df_on_index(df_new, df_12_months_ago, 'COL0')
    df_new = reorder_cols(df_new)
    return df_new

def get_df_row(df, date, col_name):

    # match on year and month only and don't worry about the day
    df = df.loc[(df['DATE'].dt.to_period('M') == date.to_period('M'))].T    
    df = df.iloc[1: , :]    
    df.rename(columns={ df.columns[0]: col_name }, inplace = True)
    df.reset_index(inplace=True)
    df = df.rename(columns = {'index':'COL0'})
    #in calse df has extra columns, make sure only to return COL0 and col_name fields each time
    return df.filter(['COL0',col_name])

def reorder_cols(df):
    cols = list(df)
    # move the column to head of list
    cols.insert(0, cols.pop(cols.index('COL0')))
    cols.insert(1, cols.pop(cols.index('LAST')))
    cols.insert(2, cols.pop(cols.index('6M')))
    cols.insert(3, cols.pop(cols.index('12M')))
    # reorder
    df = df[cols]
    return df

############################################
# Get US Lagging and Coincident Indicators #
############################################
"""
"""
sheet_name = 'DB US Lagging Indicators'

# Get last US GDP Number (QoQ, YoY). Then get GDP numbers for 6m and 12m ago from last
df_GDPC1 = get_data_fred('GDPC1', 'GDP', 'Q')
df_us_gdp = get_data(df_GDPC1)

#US  Core CPI
df_CPILFESL = get_data_fred('CPILFESL', 'CORE_CPI', 'M')
df_core_cpi = get_data(df_CPILFESL)

#US  Core PCE
df_PCEPILFE = get_data_fred('PCEPILFE', 'CORE_PCE', 'M')
df_core_pce = get_data(df_PCEPILFE)

#TODO: Add Core Retail Sales and Retail Sales

#US Retail Sales Ex Auto and Gas
df_MARTSSM44W72USS = get_data_fred('MARTSSM44W72USS','RETAIL_SALES','M')
df_retail_sales = get_data(df_MARTSSM44W72USS)

#US Unemployment Rate
df_UNRATE = get_data_fred('UNRATE', 'UNEMPLOYMENT_RATE','M')
df_unemployment_rate = get_data(df_UNRATE)

#US NFP
df_PAYEMS = get_stlouisfed_data('PAYEMS')
df_nfp = get_data(df_PAYEMS)

#US Weekly Claims
df_ICSA = get_stlouisfed_data('ICSA')
df_weekly_claims = get_data(df_ICSA)

#US Industrial Production
df_INDPRO = get_data_fred('INDPRO', 'INDUSTRIAL_PRODUCTION','M')
df_industrial_production = get_data(df_INDPRO)

# Write to excel file

df_temp = df_us_gdp.append(df_core_cpi, ignore_index=True)
df_temp = df_temp.append(df_core_pce, ignore_index=True)
df_temp = df_temp.append(df_retail_sales, ignore_index=True)
df_temp = df_temp.append(df_unemployment_rate, ignore_index=True)
df_temp = df_temp.append(df_nfp, ignore_index=True)
df_temp = df_temp.append(df_weekly_claims, ignore_index=True)
df_temp = df_temp.append(df_industrial_production, ignore_index=True)

#Select Final Columns
df_lagging_indicators = df_temp.loc[df_temp['COL0'] == 'GDP_QoQ']
df_lagging_indicators = df_lagging_indicators.append(df_temp.loc[df_temp['COL0'] == 'GDP_YoY'],True)
df_lagging_indicators = df_lagging_indicators.append(df_temp.loc[df_temp['COL0'] == 'CORE_CPI_MoM'],True)
df_lagging_indicators = df_lagging_indicators.append(df_temp.loc[df_temp['COL0'] == 'CORE_PCE_MoM'],True)
df_lagging_indicators = df_lagging_indicators.append(df_temp.loc[df_temp['COL0'] == 'RETAIL_SALES_MoM'],True)
df_lagging_indicators = df_lagging_indicators.append(df_temp.loc[df_temp['COL0'] == 'UNEMPLOYMENT_RATE_MoM'],True)
df_lagging_indicators = df_lagging_indicators.append(df_temp.loc[df_temp['COL0'] == 'PAYEMS'],True) #NFP
df_lagging_indicators = df_lagging_indicators.append(df_temp.loc[df_temp['COL0'] == 'ICSA'],True) #Weekly Claims
df_lagging_indicators = df_lagging_indicators.append(df_temp.loc[df_temp['COL0'] == 'INDUSTRIAL_PRODUCTION_MoM'],True)

# Get Original Sheet and store it in a dataframe
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

df_updated = combine_df_on_index(df_original, df_lagging_indicators, 'COL0')

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, -1)

import pdb; pdb.set_trace()

##################################
# Get US Rates and Currency Data #
##################################
sheet_name = 'DB US Rates and Currency'

# Get Current Fed Funds Current Target Rate
df_current_ffr_target = get_current_ffr_target()

# Calculate Eurodollar Futures quotes for 1m, 6m, 12m
df_eurodollar_futures = get_eurodollar_futures()

# Get US Treasury Yields #
excel_file_path_013 = '/Trading_Excel_Files/02_Interest_Rates_FX/013_Yield_Curve.xlsm'
sheet_name_013 = 'Database'

# Get Original Sheet and store it in a dataframe
df_data_013 = convert_excelsheet_to_dataframe(excel_file_path_013, sheet_name_013, True)

df_us_treasury_yields = get_data(df_data_013)

# Get DXY for Last, 6m, 12m
excel_file_path_001 = '/Trading_Excel_Files/01_Lagging_Coincident_Indicators/001_Lagging_Indicator_YoY_Asset_Class_Performance.xlsm'
sheet_name_001 = 'Database'

# Get Original Sheet and store it in a dataframe
df_data_001 = convert_excelsheet_to_dataframe(excel_file_path_001, sheet_name_001, True)
df_dxy_001 = df_data_001.filter(['DATE','DX-Y.NYB'])

#Rename column
df_dxy_001 = df_dxy_001.rename(columns={"DX-Y.NYB": "DXY"})

df_dxy = get_data(df_dxy_001)

#print(df_current_ffr_target)
#print(df_eurodollar_futures)
#print(df_us_treasury_yields)
#print(df_dxy)

# Get Original Sheet and store it in a dataframe
#df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

#df_updated = combine_df_on_index(df_original, df_us_rates_currency, 'COL0')

# Write the updated df back to the excel sheet
#write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, -1)

#############################
# Get US Leading Indicators #
#############################
sheet_name = 'DB US Leading Indicators'

excel_file_path_015 = '/Trading_Excel_Files/03_Leading_Indicators/015_Leading_Indicator_US_LEI_Consumer_Confidence.xlsm'
sheet_name_015 = 'DB LEI'

# Get Original Sheet and store it in a dataframe
df_data_015 = convert_excelsheet_to_dataframe(excel_file_path_015, sheet_name_015, True)

df_lei_015 = df_data_015.filter(['DATE','LEI']).dropna()
df_umcsi_015 = df_data_015.filter(['DATE','UMCSI']).dropna()
df_exp_015 = df_data_015.filter(['DATE','EXPECTED']).dropna()

df_lei = get_data(df_lei_015)
df_umcsi = get_data(df_umcsi_015)
df_exp = get_data(df_exp_015)

# Building Permits
excel_file_path_020 = '/Trading_Excel_Files/03_Leading_Indicators/020_Leading_Indicator_US_Housing_Market.xlsm'
sheet_name_020 = 'Database New'

# Get Original Sheet and store it in a dataframe
df_permits_020 = convert_excelsheet_to_dataframe(excel_file_path_020, sheet_name_020, True)
df_permits_020 = df_permits_020.filter(['DATE','PERMIT']).dropna()

df_permits = get_data(df_permits_020)

# ISM Manufacturing, ISM Manuf New Orders
excel_file_path_016 = '/Trading_Excel_Files/03_Leading_Indicators/016_Leading_Indicator_US_ISM_Manufacturing.xlsm'
sheet_name_016 = 'DB Details'

# Get Original Sheet and store it in a dataframe
df_ism_man_016 = convert_excelsheet_to_dataframe(excel_file_path_016, sheet_name_016, True)
df_ism_man_016 = df_ism_man_016.filter(['DATE','ISM', 'NEW_ORDERS'])

df_ism_016 = df_ism_man_016.filter(['DATE','ISM']).dropna()
df_new_orders_016 = df_ism_man_016.filter(['DATE','NEW_ORDERS']).dropna()

df_ism = get_data(df_ism_016)
df_new_orders = get_data(df_new_orders_016)

# ISM Services
excel_file_path_017 = '/Trading_Excel_Files/03_Leading_Indicators/017_Leading_Indicator_US_ISM_Services.xlsm'
sheet_name_017 = 'DB Details'

# Get Original Sheet and store it in a dataframe
df_ism_ser_017 = convert_excelsheet_to_dataframe(excel_file_path_017, sheet_name_017, True)
df_ism_ser_017 = df_ism_ser_017.filter(['DATE','ISM_SERVICES']).dropna()
df_ism_ser = get_data(df_ism_ser_017)

# Money Supply M1
df_M1REAL = get_data_fred('M1REAL','M1','M')
df_m1 = get_data(df_M1REAL)

# Money Supply M2
df_M2REAL = get_data_fred('M2REAL','M2','M')
df_m2 = get_data(df_M2REAL)

#print(df_lei)
#print(df_umcsi)
#print(df_exp)
#print(df_permits)
#print(df_ism)
#print(df_new_orders)
#print(df_ism_ser)
#print(df_m1)
#print(df_m2)

# Get Original Sheet and store it in a dataframe
#df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

#df_updated = combine_df_on_index(df_original, df_us_leading_indicators, 'COL0')

# Write the updated df back to the excel sheet
#write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, -1)

#################################
# Get ISM Manufacturing Sectors #
#################################
sheet_name = 'DB ISM Manufacturing Sectors'


###############################
# Get PMI Manufacturing World #
###############################
sheet_name = 'DB PMI Manufacturing World'


print("Done!")