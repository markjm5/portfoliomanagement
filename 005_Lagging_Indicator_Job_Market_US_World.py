import requests
import os.path
import csv
import pandas as pd
from datetime import date
from common import get_stlouisfed_data, get_oecd_data, write_to_directory, convert_excelsheet_to_dataframe, combine_df, write_dataframe_to_excel, util_check_diff_list

excel_file_path = '/trading_excel_files/01_lagging_coincident_indicators/005_Lagging_Indicator_Job_Market_US_World.xlsm'
sheet_name = 'Database'

##################################
#   Get Data from St Louis Fed   #
##################################

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

##########################
#   Get Data from OECD   #
##########################

sheet_name = 'Data World'

country = ['AUS','AUT','BEL','CAN','CHL','CZE','DEU','DNK','ESP','EST','FIN','FRA','GBR','GRC','HUN','IRL','ISL','ISR','ITA','JPN','KOR','LUX','LVA','MEX','NLD','NOR','OECD','POL','PRT','SVK','SVN','SWE','TUR','USA','EA19','G-7','CHE']

subject = ['LRHUTTTT']
measure = ['STSA']
frequency = 'M'
startDate = '1955-Q1'

todays_date = date.today()
endDate = '%s-Q4' % (todays_date.year)

#TODO: CHANGE THIS FUNCTION SO THAT IT SHOWS MONTHS INSTEAD OF QUARTERS
df_unemployed_world = get_oecd_data('STLABOUR', [country, subject, measure, [frequency]], {'startTime': startDate, 'endTime': endDate, 'dimensionAtObservation': 'AllDimensions','filename': '005_Job_Market_World.xml'})

df_unemployed_world = df_unemployed_world.drop('MTH', 1)

df_original_unemployed_world = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

# Check for difference between original and new lists
#print(util_check_diff_list(df_unemployed_world.columns.tolist(), df_original_unemployed_world.columns.tolist()))

df_updated_unemployed_world = combine_df(df_original_unemployed_world, df_unemployed_world)

# get a list of columns
cols = list(df_updated_unemployed_world)
# move the column to head of list using index, pop and insert
cols.insert(0, cols.pop(cols.index('DATE')))

# reorder
df_updated_unemployed_world = df_updated_unemployed_world[cols]

# format date
df_updated_unemployed_world['DATE'] = pd.to_datetime(df_updated_unemployed_world['DATE'],format='%d/%m/%Y')

#Write to a csv file in the correct directory
#write_to_directory(df_unemployed_world,'005_Lagging_Indicator_Job_Market_World.csv')

print("Done!")
