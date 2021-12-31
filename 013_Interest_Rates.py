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
from common import get_oecd_data, convert_excelsheet_to_dataframe, write_dataframe_to_excel, combine_df, write_to_directory, util_check_diff_list, scrape_world_gdp_table

excel_file_path = '/Trading_Excel_Files/02_Interest_Rates_FX/013_Interest_Rates.xlsm'

###################################
# Get Database 10y Data from OECD #
###################################

country = ['AUS','AUT','BEL','CAN','CHL','CZE','DEU','DNK','ESP','EST','FIN','FRA','GBR','GRC','HUN','IRL','ISL','ISR','ITA','JPN','KOR','LUX','LVA','MEX','NLD','NOR','OECD','POL','PRT','SVK','SVN','SWE','USA','EA19','EU27_2020','G-7','CHE','IND','ZAF','RUS','CHN','TUR','BRA']

#TODO: update params to get 10y rates from OECD

subject = ['IRLTLT01']
measure = ['ST']
frequency = 'M'
startDate = '1950-Q1'

todays_date = date.today()
endDate = '%s-Q4' % (todays_date.year)

df_db_10y = get_oecd_data('KEI', [country, subject, measure, [frequency]], {'startTime': startDate, 'endTime': endDate, 'dimensionAtObservation': 'AllDimensions','filename': '013_Interest_Rates.xml'})

import pdb; pdb.set_trace()

sheet_name = 'Database 10y'
df_original_db_10y = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

# Need to remove additional unnecessary rows from beginning of df_db_10y dataframe
df_db_10y = df_db_10y.iloc[1: , :].reset_index(drop=True)

"""
# Check for difference between original and new lists
print(Diff(df_QoQ.columns.tolist(), df_original.columns.tolist()))
"""
df_updated_db_10y = combine_df(df_original_db_10y, df_db_10y)

# get a list of columns
cols = list(df_updated_db_10y)
# move the column to head of list using index, pop and insert
cols.insert(0, cols.pop(cols.index('QTR')))
cols.insert(1, cols.pop(cols.index('DATE')))

# reorder
df_updated_db_10y = df_updated_db_10y[cols]

# format date
df_updated_db_10y['DATE'] = pd.to_datetime(df_updated_db_10y['DATE'],format='%d/%m/%Y')

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated_db_10y, False, 1)

print("Done!")
