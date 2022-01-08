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
from common import combine_df_on_index, get_yf_data

excel_file_path = '/Trading_Excel_Files/02_Interest_Rates_FX/013_Interest_Rates.xlsm'

def scrape_table_country_rating(url):

    page = requests.get(url=url)
    soup = BeautifulSoup(page.content, 'html.parser')

    #TODO: Need to scrape table for world production countries and numbers.
    table = soup.find('table')

    table_rows = table.find_all('tr', recursive=False)
    #table_rows = table.find_all('tr', attrs={'class':'an-estimate-row'})
    table_rows_header = table.find_all('tr')[0].find_all('th')
    df = pd.DataFrame()
    index = 0
    for header in table_rows_header:
        if(index == 0):
            df.insert(0,"Country",[],True)
        else:
            df.insert(index,str(header.text).strip().replace("'", ""),[],True)
        index+=1

    #Get rows of data.
    for tr in table_rows:
        temp_row = []
        #first_col = True
        index = 0
        td = tr.find_all('td')

        if(td):        
            for obs in td:
                text = str(obs.text).strip()
                temp_row.append(text)        
                index += 1

            df.loc[len(df.index)] = temp_row

    df = df.drop(['TE'], axis=1)
    return df

###################################
# Get Database 10y Data from OECD #
###################################

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

############################################
# Get 10y database Data from Investing.com #
############################################

# TODO: use below country list to get data and create df. Be mindful of order of countries, because it is used in 'Big Picture' sheet to load data
# mexico = https://www.investing.com/rates-bonds/mexico-10-year-historical-data
#country_list = ['u.s.','canada','brazil','germany','france','italy','spain','portugal','netherlands','austria','greece','denmark','sweden','norway','switzerland','russia','turkey','poland','hungary','czech-republic','south-africa','japan','australia','singapore','china','hong-kong','india','indonesia','south-korea','philippines','thailand','vietnam','uk','new-zealand', 'mexico']

sheet_name = 'DB 10y'
df_original_invest_10y = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True, None,'%d/%m/%Y')

#df_original_invest_10y = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True, None,'%b %d, %Y')
#write_dataframe_to_excel(excel_file_path, sheet_name, df_original_invest_10y, False, 0)

country_list = ['u.s.','canada','brazil','mexico','germany','france','italy','spain','portugal','netherlands','austria','greece','norway','switzerland','u.k.','russia','turkey','poland','hungary','czech republic','south africa','japan','australia','new zealand','singapore','china','hong kong','india','indonesia','south korea','philippines','thailand','vietnam']
#country_list = ['u.s.'] 

#country_missing = ['denmark','sweden']
df_invest_10y = get_invest_data(country_list, '10', '28/12/2000')

df_updated_invest_10y = combine_df_on_index(df_original_invest_10y, df_invest_10y, 'DATE')

# get a list of columns
cols = list(df_updated_invest_10y)
# move the column to head of list using index, pop and insert
cols.insert(0, cols.pop(cols.index('DATE')))

# reorder
df_updated_invest_10y = df_updated_invest_10y[cols]

# format date
#df_updated_invest_10y['DATE'] = pd.to_datetime(df_updated_invest_10y['DATE'],format='%b %d, %Y')

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated_invest_10y, False, 0)

###############################
# Get EURUSD Data from YF.com #
###############################

sheet_name = 'EURUSD'
df_original_EURUSD = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True, None,'%d/%m/%Y')

#get date range
todays_date = date.today()
date_str = "%s-%s-%s" % (todays_date.year, todays_date.month, todays_date.day)

# Get EUR/USD close day intervals using above date range
df_EURUSD = get_yf_data("EURUSD=X", "1d", "2000-12-28", date_str)

#Remove unnecessary columns from df_EUR_USD and rename columns
df_EURUSD = df_EURUSD.drop(['Open', 'High', 'Low', 'Volume'], axis=1)
df_EURUSD = df_EURUSD.rename(columns={"Close": "EURUSD"})

df_updated_EURUSD = combine_df_on_index(df_original_EURUSD, df_EURUSD, 'DATE')

write_dataframe_to_excel(excel_file_path, sheet_name, df_updated_EURUSD, False, 0)

###########################################
# Get 2y database Data from Investing.com #
###########################################

# TODO: use below country list to get data and create df. Be mindful of order of countries, because it is used in 'Big Picture' sheet to load data
# mexico = https://www.investing.com/rates-bonds/mexico-10-year-historical-data
#country_list = ['u.s.','canada','brazil','germany','france','italy','spain','portugal','netherlands','austria','greece','denmark','sweden','norway','switzerland','russia','turkey','poland','hungary','czech-republic','south-africa','japan','australia','singapore','china','hong-kong','india','indonesia','south-korea','philippines','thailand','vietnam','uk','new-zealand', 'mexico']

sheet_name = 'DB 2y' #TODO: Make sure excel file 013 has updated sheet name
#df_original_invest_2y = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True, None,'%b %d, %Y')
df_original_invest_2y = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True, None,'%d/%m/%Y')
#write_dataframe_to_excel(excel_file_path, sheet_name, df_original_invest_2y, False, 0)

#TODO: match country list with what is in excel file, without the missing_country list.
country_list = ['u.s.','canada','brazil','germany','france','italy','spain','portugal','netherlands','austria','norway','switzerland', 'u.k.','russia','turkey','poland','czech republic','south africa','japan','australia','new zealand','singapore','china','hong kong','india','south korea','philippines','thailand','vietnam']
#country_missing = ['denmark','sweden', 'mexico', 'greece', 'hungary', 'indonesia']

df_invest_2y = get_invest_data(country_list, '2', '28/12/2000')
#df_invest_2y = get_invest_data(country_missing, '2', '28/12/2000')

df_updated_invest_2y = combine_df_on_index(df_original_invest_2y, df_invest_2y, 'DATE')

# get a list of columns
cols = list(df_updated_invest_2y)
# move the column to head of list using index, pop and insert
cols.insert(0, cols.pop(cols.index('DATE')))

# reorder
df_updated_invest_2y = df_updated_invest_2y[cols]

# format date
#df_updated_invest_2y['DATE'] = pd.to_datetime(df_updated_invest_2y['DATE'],format='%b %d, %Y')

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated_invest_2y, False, 0)

#################################################
# Get Credit Rating Data from Trading Economics #
#################################################

#https://tradingeconomics.com/country-list/rating

sheet_name = 'DB Credit Rating'

#Get World Production Data
df_country_credit_rating = scrape_table_country_rating("https://tradingeconomics.com/country-list/rating")

df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name)

df_updated = combine_df_on_index(df_original, df_country_credit_rating, 'Country')

write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, -1)

print("Done!")
