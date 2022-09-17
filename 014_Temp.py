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
from common import convert_excelsheet_to_dataframe, write_dataframe_to_excel
from common import combine_df_on_index

#TODO: Preparation: Load data from each country tab from Cell A5 into dataframe, merge based on date, and write to new excel sheet
excel_file_path = '/Trading_Excel_Files/02_Interest_Rates_FX/014_FX_Commitment_of_Traders_History.xlsm'

def return_df(sheet_name):
    df = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)
    return df

sheet_name = "EUR"
df_eur = return_df(sheet_name)

sheet_name = "CAD"
df_cad = return_df(sheet_name)

sheet_name = "MEX"
df_mex = return_df(sheet_name)

sheet_name = "JPY"
df_jpy = return_df(sheet_name)

sheet_name = "NZD"
df_nzd = return_df(sheet_name)

sheet_name = "ZAR"
df_zar = return_df(sheet_name)

sheet_name = "AUD"
df_aud = return_df(sheet_name)

sheet_name = "CHF"
df_chf = return_df(sheet_name)

sheet_name = "GBP"
df_gbp = return_df(sheet_name)

sheet_name = "RUB"
df_rub = return_df(sheet_name)

sheet_name = "BRL"
df_brl = return_df(sheet_name)

sheet_name_original = "Database"
#import pdb; pdb.set_trace()
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name_original, True)

#Combine new data with original data
df_original = combine_df_on_index(df_eur, df_cad, 'DATE')
df_original = combine_df_on_index(df_original, df_mex, 'DATE')
df_original = combine_df_on_index(df_original, df_jpy, 'DATE')
df_original = combine_df_on_index(df_original, df_nzd, 'DATE')
df_original = combine_df_on_index(df_original, df_zar, 'DATE')
df_original = combine_df_on_index(df_original, df_aud, 'DATE')
df_original = combine_df_on_index(df_original, df_chf, 'DATE')
df_original = combine_df_on_index(df_original, df_gbp, 'DATE')
df_original = combine_df_on_index(df_original, df_rub, 'DATE')
df_original = combine_df_on_index(df_original, df_brl, 'DATE')

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name_original, df_original, False, 0)

print("Done!")