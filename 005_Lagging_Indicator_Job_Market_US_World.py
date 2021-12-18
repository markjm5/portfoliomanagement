import requests
import os.path
import csv
import pandas as pd
from datetime import date
from common import get_stlouisfed_data, get_oecd_data, write_to_directory, convert_excelsheet_to_dataframe, combine_df, write_dataframe_to_excel

excel_file_path = '/trading_excel_files/01_lagging_coincident_indicators/005_Lagging_Indicator_Job_Market_US_World.xlsm'
sheet_name = 'Database'

df_CIVPART = get_stlouisfed_data('CIVPART')
df_PAYEMS = get_stlouisfed_data('PAYEMS')
df_UNRATE = get_stlouisfed_data('UNRATE')

df_1 = pd.merge(df_CIVPART,df_PAYEMS,"right")
df_1 = pd.merge(df_1,df_UNRATE,"left")

df_original_1 = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)
df_updated_1 = combine_df(df_original_1, df_1)

write_dataframe_to_excel(excel_file_path, sheet_name, df_updated_1, False, 0)

sheet_name = 'Database Claims'

df_CCSA = get_stlouisfed_data('CCSA')
df_ICSA = get_stlouisfed_data('ICSA')

#Combine all these data frames into a single data frame
df_2 = pd.merge(df_CCSA,df_ICSA,"right")

df_original_2 = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)
df_updated_2 = combine_df(df_original_2, df_2)

write_dataframe_to_excel(excel_file_path, sheet_name, df_updated_2, False, 0)

#df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

#df_unemployed_US = pd.concat([df_1,df_2],axis=1)

#Write to a csv file in the correct directory
#write_to_directory(df_unemployed_US,'005_Lagging_Indicator_Job_Market_US.csv')

import pdb; pdb.set_trace()

#TODO: Get Correct Data from OECD
#https://stats.oecd.org/restsdmx/sdmx.ashx/GetData/MEI_ARCHIVE/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LTU+LVA+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+NMEC+BRA+RUS.501.202102.Q+M/all?startTime=2020-Q4&endTime=2021-Q4
#'https://stats.oecd.org/restsdmx/sdmx.ashx/GetData/MEI_ARCHIVE/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LTU+LVA+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+EA19+EU27_2020+G-7+NAFTA+OECDE+G-20+OECD+ARG+BRA+BGR+CHN+IND+IDN+ROU+RUS+SAU+ZAF.501.202102.M/all?startTime=1955-Q1&endTime=2021-Q4'
# THIS MIGHT BE THE BEST: https://stats.oecd.org/restsdmx/sdmx.ashx/GetData/STLABOUR/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LTU+LVA+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+EA19+EU27_2020+G-7+OECD.LRHUTTFE+LRHUTTMA+LRHUTTTT.STSA.Q+M/all?startTime=2020-Q4&endTime=2021-Q4
country = ['AUS','AUT','BEL','CAN','CHL','CZE','DEU','DNK','ESP','EST','FIN','FRA','GBR','GRC','HUN','IRL','ISL','ISR','ITA','JPN','KOR','LUX','LVA','MEX','NLD','NOR','OECD','POL','PRT','SVK','SVN','SWE','TUR','USA','EA19','G-7','CHE']

subject = ['LRHUTTTT']
measure = ['STSA']
frequency = 'M'
startDate = '1955-Q1'

todays_date = date.today()
endDate = '%s-Q4' % (todays_date.year)

#TODO: CHANGE THIS FUNCTION SO THAT IT SHOWS MONTHS INSTEAD OF QUARTERS
df_unemployed_world = get_oecd_data('STLABOUR', [country, subject, measure, [frequency]], {'startTime': startDate, 'endTime': endDate, 'dimensionAtObservation': 'AllDimensions','filename': '005_Job_Market_World.xml'})
#import pdb; pdb.set_trace()
#Write to a csv file in the correct directory
write_to_directory(df_unemployed_world,'005_Lagging_Indicator_Job_Market_World.csv')

print("Done!")
