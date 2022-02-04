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
import investpy
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

etfs = [ 'RXI','XLP','XLY','XLE','XLF','XLV','XLI','XLK','XLB','XLRE','XTL','XLU','SPY','USO','QQQ','IWM','IBB','EEM','HYG','VNQ','MDY','SLY','EFA','TIP','AGG','DJP','BIL','GC=F','DX-Y.NYB']

#TODO: Get the following data from YF

for etf in etfs:
    print("Getting Data For: %s" % (etf,))
    df_etf = get_yf_data(etf, "1d", "2000-12-28", date_str)

    #Remove unnecessary columns and rename columns
    df_etf = df_etf.drop(['Open', 'High', 'Low', 'Volume'], axis=1)
    df_etf = df_etf.rename(columns={"Close": etf})

    df_etf_data = combine_df_on_index(df_etf_data, df_etf, 'DATE')



#TODO: Calculate Annual Returns on each asset class. Look at 007, 013 for calculation examples
import pdb; pdb.set_trace()


#write_dataframe_to_excel(excel_file_path, sheet_name, df_updated_EURUSD, False, 0)


print("Done!")
