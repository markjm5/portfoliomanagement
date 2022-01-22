import requests
import os.path
import csv
import pandas as pd
from common import get_stlouisfed_data, convert_excelsheet_to_dataframe, write_dataframe_to_excel, combine_df_on_index

excel_file_path = '/Trading_Excel_Files/01_Lagging_Coincident_Indicators/006_Lagging_Indicator_Personal_Consumption_Expenditures.xlsm'
sheet_name = 'Database'

df_PCEPI = get_stlouisfed_data('PCEPI')
df_PCEPILFE = get_stlouisfed_data('PCEPILFE')
df_DFEDTARU = get_stlouisfed_data('DFEDTARU')
df_CPILFESL = get_stlouisfed_data('CPILFESL')

#Combine all these data frames into a single data frame based on the DATE field

df = combine_df_on_index(df_PCEPI,df_PCEPILFE,"DATE")
df = combine_df_on_index(df_DFEDTARU,df,"DATE")
df = combine_df_on_index(df_CPILFESL,df,"DATE")

df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)
df_updated = combine_df_on_index(df_original, df, 'DATE')

# get a list of columns
cols = list(df_updated)
# move the column to head of list
cols.insert(0, cols.pop(cols.index('DATE')))
cols.insert(1, cols.pop(cols.index('PCEPI')))
cols.insert(2, cols.pop(cols.index('PCEPILFE')))
cols.insert(3, cols.pop(cols.index('DFEDTARU')))
cols.insert(4, cols.pop(cols.index('CPILFESL')))

# reorder
df_updated = df_updated[cols]

write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

#LEGACY Write to a csv file in the correct directory
#write_to_directory(df,'006_Lagging_Indicator_Personal_Consumption_Expenditures.csv')

print("Done!")