from datetime import date
import pandas as pd
from common import get_oecd_data, convert_excelsheet_to_dataframe, write_dataframe_to_excel, combine_df, write_to_directory, util_check_diff_list

excel_file_path = '/Trading_Excel_Files/01_Lagging_Coincident_Indicators/008_Lagging_Indicator_Global_Inflation.xlsm'

#####################################
#   Get Global CPI Data from OECD   #
#####################################

sheet_name = "Data"

# TODO: Get OECD Data Using API: https://stackoverflow.com/questions/40565871/read-data-from-oecd-api-into-python-and-pandas
# https://stats.oecd.org/restsdmx/sdmx.ashx/GetData/QNA/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LTU+LVA+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+EA19+EU27_2020+G-7+NAFTA+OECDE+G-20+OECD+ARG+BRA+BGR+CHN+IND+IDN+ROU+RUS+SAU+ZAF.B1_GE+P31S14_S15+P3S13+P51+P52_P53+B11+P6+P7.GYSA+GPSA+CTQRGPSA.Q/all?startTime=2019-Q3&endTime=2021-Q3
# https://stats.oecd.org/restsdmx/sdmx.ashx/GetData/QNA/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LTU+LVA+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+EA19+EU27_2020+G-7+NAFTA+OECDE+G-20+OECD+ARG+BRA+BGR+CHN+IND+IDN+ROU+RUS+SAU+ZAF.B1_GE+P31S14_S15+P3S13+P51+P52_P53+B11+P6+P7.GYSA+GPSA+CTQRGPSA.Q/all?startTime=2019-Q3&endTime=2021-Q3
#Get CPI Data from OECD
country = ['AUT','BEL','BRA','CAN','CHE','CHL','CHN','COL','CRI','CZE','DEU','DNK','EA19','ESP','EST','EU27_2020','FIN','FRA','G-20','G-7','GBR','GRC','HUN','IDN','IND','IRL','ISL','ISR','ITA','JPN','KOR','LTU','LUX','LVA','MEX','NLD','NOR','OECD','OECDE','POL','PRT','RUS','SAU','SVK','SVN','SWE','TUR','USA','ZAF']
subject = ['CPALTT01']
measure = ['IXOB']
frequency = 'M'
startDate = '1949-Q1'

todays_date = date.today()
endDate = '%s-Q4' % (todays_date.year)

df_global_cpi = get_oecd_data('PRICES_CPI', [country, subject, measure, [frequency]], {'startTime': startDate, 'endTime': endDate, 'dimensionAtObservation': 'AllDimensions','filename': '008_Global_CPI.xml'})
df_global_cpi = df_global_cpi.drop('MTH', 1)

df_original_global_cpi = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

"""
#Make sure all object fields are numeric
for col in df_original_global_cpi.columns:
    if df_original_global_cpi[col].dtype == object:
        print(col)
        df_original_global_cpi[col] = pd.to_numeric(df_original_global_cpi[col])
"""

# Check for difference between original and new lists
#print(util_check_diff_list(df_global_cpi.columns.tolist(), df_original_global_cpi.columns.tolist()))

df_updated_global_cpi = combine_df(df_original_global_cpi, df_global_cpi)

# get a list of columns
cols = list(df_updated_global_cpi)
# move the column to head of list using index, pop and insert
cols.insert(0, cols.pop(cols.index('DATE')))

# reorder
df_updated_global_cpi = df_updated_global_cpi[cols]

# format date
df_updated_global_cpi['DATE'] = pd.to_datetime(df_updated_global_cpi['DATE'],format='%d/%m/%Y')

write_dataframe_to_excel(excel_file_path, sheet_name, df_updated_global_cpi, False, 0)

#Write to a csv file in the correct directory
#write_to_directory(df_global_cpi,'008_Lagging_Indicator_World_CPI.csv')

##########################################
#   Get Global Core CPI Data from OECD   #
##########################################

#https://stats.oecd.org/restsdmx/sdmx.ashx/GetData/QNA/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LTU+LVA+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+EA19+EU27_2020+G-7+NAFTA+OECDE+G-20+OECD+ARG+BRA+BGR+CHN+IND+IDN+ROU+RUS+SAU+ZAF.B1_GE+P31S14_S15+P3S13+P51+P52_P53+B11+P6+P7.GYSA+GPSA+CTQRGPSA.Q/all?startTime=2019-Q3&endTime=2021-Q3

sheet_name = "Data Core CPI"

country = ['AUT','BEL','BRA','CAN','CHE','CHL','CHN','COL','CRI','CZE','DEU','DNK','EA19','ESP','EST','FIN','FRA','G-20','G-7','GBR','GRC','HUN','IDN','IND','IRL','ISL','ISR','ITA','JPN','KOR','LTU','LUX','LVA','MEX','NLD','NOR','OECD','OECDE','POL','PRT','RUS','SAU','SVK','SVN','SWE','TUR','USA','ZAF']
subject = ['CPGRLE01']
measure = ['IXOB']
frequency = 'M'
startDate = '1949-Q1'

todays_date = date.today()
endDate = '%s-Q4' % (todays_date.year)

df_global_core_cpi = get_oecd_data('PRICES_CPI', [country, subject, measure, [frequency]], {'startTime': startDate, 'endTime': endDate, 'dimensionAtObservation': 'AllDimensions','filename': '008_Global_Core_CPI.xml'})

df_global_core_cpi = df_global_core_cpi.drop('MTH', 1)

df_original_global_core_cpi = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

# Check for difference between original and new lists
#print(util_check_diff_list(df_global_core_cpi.columns.tolist(), df_original_global_core_cpi.columns.tolist()))

df_updated_global_core_cpi = combine_df(df_original_global_core_cpi, df_global_core_cpi)

# get a list of columns
cols = list(df_updated_global_core_cpi)
# move the column to head of list using index, pop and insert
cols.insert(0, cols.pop(cols.index('DATE')))

# reorder
df_updated_global_core_cpi = df_updated_global_core_cpi[cols]

#Write to a csv file in the correct directory
#write_to_directory(df_global_core_cpi,'008_Lagging_Indicator_World_Core_CPI.csv')

write_dataframe_to_excel(excel_file_path, sheet_name, df_updated_global_core_cpi, False, 0)

print("Done!")
