import requests
import os.path
import csv
import pandas as pd
from common import get_stlouisfed_data, convert_excelsheet_to_dataframe, write_dataframe_to_excel, combine_df, write_to_directory

excel_file_path = '/Trading_Excel_Files/01_Lagging_Coincident_Indicators/006_Lagging_Indicator_Personal_Consumption_Expenditures.xlsm'
sheet_name = 'Database'

df_PCEPI = get_stlouisfed_data('PCEPI')
df_PCEPILFE = get_stlouisfed_data('PCEPILFE')
df_DFEDTARU = get_stlouisfed_data('DFEDTARU')
df_CPILFESL = get_stlouisfed_data('CPILFESL')

#Combine all these data frames into a single data frame based on the DATE field

df = pd.merge(df_PCEPI,df_PCEPILFE,"right")
df = pd.merge(df,df_DFEDTARU,"left")
df = pd.merge(df,df_CPILFESL,"left")

df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)
df_updated = combine_df(df_original, df)

write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

#LEGACY Write to a csv file in the correct directory
#write_to_directory(df,'006_Lagging_Indicator_Personal_Consumption_Expenditures.csv')

print("Done!")