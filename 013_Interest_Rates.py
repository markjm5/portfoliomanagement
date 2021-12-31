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
import time
from common import get_oecd_data, convert_excelsheet_to_dataframe, write_dataframe_to_excel, combine_df, write_to_directory, util_check_diff_list, scrape_world_gdp_table

excel_file_path = '/Trading_Excel_Files/02_Interest_Rates_FX/013_Interest_Rates.xlsm'

###################################
# Get Database 10y Data from OECD #
###################################
"""
country = ['AUS','AUT','BEL','CAN','CHL','CZE','DEU','DNK','ESP','EST','FIN','FRA','GBR','GRC','HUN','IRL','ISL','ISR','ITA','JPN','KOR','LUX','LVA','MEX','NLD','NOR','OECD','POL','PRT','SVK','SVN','SWE','USA','EA19','EU27_2020','G-7','CHE','IND','ZAF','RUS','CHN','TUR','BRA']
subject = ['IRLTLT01']
measure = ['ST']
frequency = 'M'
startDate = '1950-Q1'

todays_date = date.today()
endDate = '%s-Q4' % (todays_date.year)

df_db_10y = get_oecd_data('KEI', [country, subject, measure, [frequency]], {'startTime': startDate, 'endTime': endDate, 'dimensionAtObservation': 'AllDimensions','filename': '013_Interest_Rates.xml'})
df_db_10y = df_db_10y.drop('MTH', 1)

sheet_name = 'Database 10y'
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
"""
##############################################
# Scrape 2y database Data from Investing.com #
##############################################

# TODO: use below country list to get data and create df. Be mindful of order of countries, because it is used in 'Big Picture' sheet to load data
# mexico = https://www.investing.com/rates-bonds/mexico-10-year-historical-data
#country_list = ['u.s.','canada','brazil','germany','france','italy','spain','portugal','netherlands','austria','greece','denmark','sweden','norway','switzerland','russia','turkey','poland','hungary','czech-republic','south-africa','japan','australia','singapore','china','hong-kong','india','indonesia','south-korea','philippines','thailand','vietnam','uk','new-zealand', 'mexico']

country_list = ['argentina', 'australia', 'austria', 'bahrain', 'bangladesh', 'belgium', 'botswana', 'brazil', 'bulgaria', 'canada', 'chile', 'china', 'colombia', 'croatia', 'cyprus', 'czech republic', 'denmark', 'egypt', 'finland', 'france', 'germany', 'greece', 'hong kong', 'hungary', 'iceland', 'india', 'indonesia', 'ireland', 'israel', 'italy', 'japan', 'jordan', 'kenya', 'latvia', 'lithuania', 'luxembourg', 'malaysia', 'malta', 'mauritius', 'mexico', 'morocco', 'namibia', 'netherlands', 'new zealand', 'nigeria', 'norway', 'pakistan', 'peru', 'philippines', 'poland', 'portugal', 'qatar', 'romania', 'russia', 'saudi arabia', 'serbia', 'singapore', 'slovakia', 'slovenia', 'south africa', 'south korea', 'spain', 'sri lanka', 'sweden', 'switzerland', 'taiwan', 'thailand', 'turkey', 'uganda', 'ukraine', 'united kingdom', 'united states', 'venezuela', 'vietnam']

# https://pypi.org/project/investpy/
# df_example = investpy.get_stock_historical_data(stock='AAPL', country='United States', from_date='01/01/2010', to_date='01/01/2020')

for country in country_list:
    """
    if(country == 'mexico'):
        url = "https://www.investing.com/rates-bonds/mexico-10-year-historical-data"
    else:
        url = "https://www.investing.com/rates-bonds/%s-10-year-bond-yield-historical-data" % (country,)
    resp = requests.get(url=url)
    """
    bond = "%s %sy" % (country, '10')

    try:
        time.sleep(10)
        data = investpy.get_bond_historical_data(bond=bond, from_date='01/01/2010', to_date='01/01/2019')
        print(country)
        print(data.head())

    except RuntimeError as e:
        print("%s does not exist" % (bond))


import pdb; pdb.set_trace()

###############################################
# Scrape 10y database Data from Investing.com #
###############################################

# TODO
