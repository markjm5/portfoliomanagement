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

excel_file_path = '/trading_excel_files/01_lagging_coincident_indicators/003_Lagging_Indicator_World_GDP.xlsm'

"""
def scrape_world_gdp_table(url):
  #Scrape GDP Table from Trading Economics
  #url = "https://tradingeconomics.com/matrix"

  page = requests.get(url=url)
  soup = BeautifulSoup(page.content, 'html.parser')

  table = soup.find('table')
  table_rows = table.find_all('tr', attrs={'align':'center'})
  table_rows_header = table.find_all('tr')[0].find_all('th')
  df = pd.DataFrame()

  index = 0
  for header in table_rows_header:
    if(index == 0):
      df.insert(0,"Country",[],True)
    else:
      df.insert(index,header.text,[],True)

    index+=1

  #Insert New Row. Format the data to show percentage as float

  for tr in table_rows:
    temp_row = []
    first_col = True

    td = tr.find_all('td')
    for obs in td:
      if(first_col):
        text = ''.join(e for e in obs.text if e.isalnum())
        text = re.sub("([A-Z])", " \\1", text).strip()
      else:
        if(obs.text.find('%') < 0):
          text = obs.text
        else:
          text = obs.text.strip('%')
          text = float(text.strip('%'))/100

      temp_row.append(text)        
      first_col = False

    df.loc[len(df.index)] = temp_row

  return df
  """


#https://stats.oecd.org/restsdmx/sdmx.ashx/GetData/QNA/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LTU+LVA+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+EA19+EU27_2020+G-7+NAFTA+OECDE+G-20+OECD+ARG+BRA+BGR+CHN+IND+IDN+ROU+RUS+SAU+ZAF.B1_GE+P31S14_S15+P3S13+P51+P52_P53+B11+P6+P7.GYSA+GPSA+CTQRGPSA.Q/all?startTime=2019-Q3&endTime=2021-Q3

##########################
# Get QoQ Data from OECD #
##########################
#country = ['AUS','AUT','BEL','CAN','CHL','COL','CRI','CZE','DNK','EST','FIN','FRA','DEU','GRC','HUN','ISL','IRL','ISR','ITA','JPN','KOR','LTU','LVA','LUX','MEX','NLD','NZL','NOR','POL','PRT','SVK','SVN','ESP','SWE','CHE','TUR','GBR','USA','EA19','EU27_2020','G-7','NAFTA','OECDE','G-20','OECD','ARG','BRA','BGR','CHN','IND','IDN','ROU','RUS','SAU','ZAF']
#remove 'G-7', 'EU27_2020'

country = ['AUS','AUT','BEL','CAN','CHL','CZE','DNK','EST','FIN','FRA','DEU','GRC','HUN','ISL','IRL','ISR','ITA','JPN','KOR','LVA','LUX','MEX','NLD','NZL','NOR','POL','PRT','SVK','SVN','ESP','SWE','CHE','TUR','GBR','USA','EA19','EU27_2020','G-7','OECD','ARG','BRA','IND','RUS','ZAF', 'CHN']

subject = ['B1_GE']
measure = ['GPSA']
frequency = 'Q'
startDate = '1947-Q1'

todays_date = date.today()
endDate = '%s-Q4' % (todays_date.year)

df_QoQ = get_oecd_data('QNA', [country, subject, measure, [frequency]], {'startTime': startDate, 'endTime': endDate, 'dimensionAtObservation': 'AllDimensions','filename': '003_QoQ.xml'})

#Write to a csv file in the correct directory
#write_to_directory(df_QoQ,'003_Lagging_Indicator_World_GDP_QoQ.csv')

sheet_name = 'Data qoq'
df_original_QoQ = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

# Need to remove additional unnecessary rows from beginning of df_QoQ dataframe
df_QoQ = df_QoQ.iloc[1: , :].reset_index(drop=True)

"""
# Check for difference between original and new lists
print(Diff(df_QoQ.columns.tolist(), df_original.columns.tolist()))
"""
df_updated_QoQ = combine_df(df_original_QoQ, df_QoQ)

# get a list of columns
cols = list(df_updated_QoQ)
# move the column to head of list using index, pop and insert
cols.insert(0, cols.pop(cols.index('QTR')))
cols.insert(1, cols.pop(cols.index('DATE')))

# reorder
df_updated_QoQ = df_updated_QoQ[cols]

# format date
df_updated_QoQ['DATE'] = pd.to_datetime(df_updated_QoQ['DATE'],format='%d/%m/%Y')

#TODO: Compare numbers with original excel numbers and fix any discrepancies

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated_QoQ, False, 1)

##########################
# Get YoY Data from OECD #
##########################
#https://stats.oecd.org/restsdmx/sdmx.ashx/GetData/QNA/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LTU+LVA+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+EA19+EU27_2020+G-7+NAFTA+OECDE+G-20+OECD+ARG+BRA+BGR+CHN+IND+IDN+ROU+RUS+SAU+ZAF.B1_GE+P31S14_S15+P3S13+P51+P52_P53+B11+P6+P7.GYSA+GPSA+CTQRGPSA.Q/all?startTime=2019-Q3&endTime=2021-Q3

#country = ['AUS','AUT','BEL','CAN','CHL','COL','CRI','CZE','DNK','EST','FIN','FRA','DEU','GRC','HUN','ISL','IRL','ISR','ITA','JPN','KOR','LTU','LVA','LUX','MEX','NLD','NZL','NOR','POL','PRT','SVK','SVN','ESP','SWE','CHE','TUR','GBR','USA','EA19','EU27_2020','G-7','NAFTA','OECDE','G-20','OECD','ARG','BRA','BGR','CHN','IND','IDN','ROU','RUS','SAU','ZAF']
country = ['AUS','AUT','BEL','CAN','CHL','CZE','DNK','EST','FIN','FRA','DEU','GRC','HUN','ISL','IRL','ISR','ITA','JPN','KOR','LVA','LUX','MEX','NLD','NZL','NOR','POL','PRT','SVK','SVN','ESP','SWE','CHE','TUR','GBR','USA','EA19','EU27_2020','G-7','OECD','ARG','BRA','CHN','IND','RUS','ZAF']

subject = ['B1_GE']
measure = ['GYSA']
frequency = 'Q'
startDate = '1947-Q1'

todays_date = date.today()
endDate = '%s-Q4' % (todays_date.year)

df_YoY = get_oecd_data('QNA', [country, subject, measure, [frequency]], {'startTime': startDate, 'endTime': endDate, 'dimensionAtObservation': 'AllDimensions','filename': '003_YoY.xml'})

#Write to a csv file in the correct directory
#write_to_directory(df_YoY,'003_Lagging_Indicator_World_GDP_YoY.csv')

sheet_name = 'Data yoy'
df_original_YoY = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

# Need to remove additional unnecessary rows from beginning of df_YoY dataframe
df_YoY = df_YoY.iloc[1: , :].reset_index(drop=True)

# Check for difference between original and new lists
#print(util_check_diff_list(df_YoY.columns.tolist(), df_original.columns.tolist()))

df_updated_YoY = combine_df(df_original_YoY, df_YoY)

# get a list of columns
cols = list(df_updated_YoY)
# move the column to head of list using index, pop and insert
cols.insert(0, cols.pop(cols.index('QTR')))
cols.insert(1, cols.pop(cols.index('DATE')))

# reorder
df_updated_YoY = df_updated_YoY[cols]

# format date
df_updated_YoY['DATE'] = pd.to_datetime(df_updated_YoY['DATE'],format='%d/%m/%Y')

#TODO: Compare numbers with original excel numbers and fix any discrepancies

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated_YoY, False, 1)

##################################################
# Get World GDP Data from Trading Economics Site #
##################################################

sheet_name = 'Data World GDP'

df_original_world_gdp = convert_excelsheet_to_dataframe(excel_file_path, sheet_name)
df_world_gdp = scrape_world_gdp_table("https://tradingeconomics.com/matrix")
"""
print(df_original_world_gdp.head())
print(df_world_gdp.head())
print(df_original_world_gdp.tail())
print(df_world_gdp.tail())
"""
#Fix datatypes of df_world_gdp
for column in df_world_gdp:
  if(column != 'Country'):
    df_world_gdp[column] = pd.to_numeric(df_world_gdp[column])

#Combine df_original_world_gdp with df_world_gdp
df_updated_world_gdp = combine_df(df_original_world_gdp, df_world_gdp)

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated_world_gdp, False, -1)

print("Done!")
