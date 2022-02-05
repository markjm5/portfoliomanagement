from json.decoder import JSONDecodeError
import sys
import requests
import os.path
import csv
import pandas as pd
import xml.etree.ElementTree as ET
from inspect import getmembers, isclass, isfunction
from datetime import date
from bs4 import BeautifulSoup
from requests.models import parse_header_links
import re
from common import get_oecd_data, get_invest_data, convert_excelsheet_to_dataframe, write_dataframe_to_excel
from common import combine_df_on_index, get_yf_data

excel_file_path = '/Trading_Excel_Files/01_Lagging_Coincident_Indicators/001_Lagging_Indicator_YoY_Asset_Class_Performance.xlsm'

####################################
# Get Asset Class Data from YF.com #
####################################

sheet_name = 'Database'
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True, None,'%d/%m/%Y')

data = {'DATE': []}

# Convert the dictionary into DataFrame
df_etf_data = pd.DataFrame(data)

#get date range
todays_date = date.today()
date_str = "%s-%s-%s" % (todays_date.year, todays_date.month, todays_date.day)

etfs = [ 'RXI','XLP','XLY','XLE','XLF','XLV','XLI','XLK','XLB','XLRE','XLC','XLU','SPY','USO','QQQ','IWM','IBB','EEM','HYG','VNQ','MDY','SLY','EFA','TIP','AGG','DJP','BIL','GC=F','DX-Y.NYB']

for etf in etfs:
    print("Getting Data For: %s" % (etf,))
    df_etf = get_yf_data(etf, "1d", "2007-01-01", date_str)

    #Remove unnecessary columns and rename columns
    df_etf = df_etf.drop(['Open', 'High', 'Low', 'Volume'], axis=1)
    df_etf = df_etf.rename(columns={"Close": etf})

    df_etf_data = combine_df_on_index(df_etf_data, df_etf, 'DATE')

df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

df_updated = combine_df_on_index(df_original, df_etf_data, 'DATE')

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)


#Calculate Annual Returns on each asset class. https://stackoverflow.com/questions/64096220/how-to-calculate-the-yearly-percent-return-in-pandas
sheet_name = 'DB Annual Returns'

data = {'DATE': []}

# Convert the dictionary into DataFrame
df_percentage_change = pd.DataFrame(data)

for etf in etfs:

    #groupby year and determine the daily percent change by year, and add it as a column to df
    df_etf_data['%s_pct_ch' % (etf,)] = df_etf_data.groupby(df_etf_data.DATE.dt.year)[etf].apply(pd.Series.pct_change)

    #Drop unnecessary columns
    df_etf_data = df_etf_data.drop(columns=etf, axis=1)

    # groupby year and aggregate sum of pct_ch to get the yearly return
    df_yearly_pct_ch = df_etf_data.groupby(df_etf_data.DATE.dt.year)['%s_pct_ch' % (etf,)].sum().mul(100).reset_index().rename(columns={'%s_pct_ch' % (etf,): etf})

    df_percentage_change = combine_df_on_index(df_percentage_change, df_yearly_pct_ch, 'DATE')


df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

df_updated = combine_df_on_index(df_original, df_percentage_change, 'DATE')

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, -1)

print("Done!")
