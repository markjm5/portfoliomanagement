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
from common import get_oecd_data, get_invest_data, convert_excelsheet_to_dataframe, write_dataframe_to_excel, combine_df, write_to_directory, util_check_diff_list, scrape_world_gdp_table
from common import combine_df_on_index, get_stlouisfed_data

excel_file_path = '/Trading_Excel_Files/02_Interest_Rates_FX/014_US_Global_Money_Supply.xlsm'

#################################################
# Get US M1, M2 Monthly Data from St Louis Fred #
#################################################

sheet_name = 'DB Money Supply'

df_M1REAL = get_stlouisfed_data('M1REAL')
df_M2REAL = get_stlouisfed_data('M2REAL')
df_M1 = get_stlouisfed_data('M1')
df_M2SL = get_stlouisfed_data('M2SL')

#Combine all these data frames into a single data frame based on the DATE field
df = combine_df_on_index(df_M1REAL, df_M2REAL, 'DATE')
df = combine_df_on_index(df_M1, df, 'DATE')
df = combine_df_on_index(df_M2SL, df, 'DATE')

#Get original data from sheet
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

#Combine new data with original data
df_updated = combine_df_on_index(df_original, df, 'DATE')

# get a list of columns
cols = list(df_updated)
# move the column to head of list using index, pop and insert
cols.insert(0, cols.pop(cols.index('DATE')))
cols.insert(1, cols.pop(cols.index('M1REAL')))
cols.insert(2, cols.pop(cols.index('M2REAL')))
cols.insert(3, cols.pop(cols.index('M1')))
cols.insert(4, cols.pop(cols.index('M2SL')))

# reorder
df_updated = df_updated[cols]

#Write to excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

import pdb; pdb.set_trace()

########################################################
# Get Monthly Money Supply Data from Trading Economics #
########################################################

#TODO: Scrape Data from Trading Economics Site


##########################################
# Get Global Money Supply Data from OECD #
##########################################

#TODO: Get data from correct OECD time series
#TODO: Update excel sheet to remove unnecessary columns

country = ['AUS','AUT','BEL','CAN','CHL','CZE','DEU','DNK','ESP','EST','FIN','FRA','GBR','GRC','HUN','IRL','ISL','ISR','ITA','JPN','KOR','LUX','LVA','MEX','NLD','NOR','OECD','POL','PRT','SVK','SVN','SWE','USA','EA19','EU27_2020','G-7','CHE','IND','ZAF','RUS','CHN','TUR','BRA']
subject = ['IRLTLT01']
measure = ['ST']
frequency = 'M'
startDate = '1950-Q1'

todays_date = date.today()
endDate = '%s-Q4' % (todays_date.year)

df_db_10y = get_oecd_data('KEI', [country, subject, measure, [frequency]], {'startTime': startDate, 'endTime': endDate, 'dimensionAtObservation': 'AllDimensions','filename': '013_Interest_Rates.xml'})
df_db_10y = df_db_10y.drop('MTH', 1)

sheet_name = 'Data'
df_original_db_10y = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

# Check for difference between original and new lists
#print(Diff(df_db_10y.columns.tolist(), df_original_db_10y.columns.tolist()))

df_updated_db_10y = combine_df(df_original_db_10y, df_db_10y)

# get a list of columns
cols = list(df_updated_db_10y)
# move the column to head of list using index, pop and insert
cols.insert(0, cols.pop(cols.index('DATE')))

# reorder
df_updated_db_10y = df_updated_db_10y[cols]

# format date
df_updated_db_10y['DATE'] = pd.to_datetime(df_updated_db_10y['DATE'],format='%Y-%m-%d')

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated_db_10y, False, 0)

print("Done!")
