import requests
import os.path
import csv
import pandas as pd
from common import get_stlouisfed_data, convert_excelsheet_to_dataframe, write_dataframe_to_excel, combine_df, write_to_directory, util_check_diff_list

excel_file_path = '/Trading_Excel_Files/01_Lagging_Coincident_Indicators/011_Lagging_Indicator_Retail_Sales.xlsm'
sheet_name = 'Database'

df_RSAFS = get_stlouisfed_data('RSAFs')
df_RSFSXMV = get_stlouisfed_data('RSFSXMV')
df_MARTSSM44W72USS = get_stlouisfed_data('MARTSSM44W72USS')
df_RSDBS = get_stlouisfed_data('RSDBs')
df_RSNSR = get_stlouisfed_data('RSNSr')
df_RSHPCS = get_stlouisfed_data('RSHPCs')
df_RSSGHBMS = get_stlouisfed_data('RSSGHBMs')
df_RSGMS = get_stlouisfed_data('RSGMs')
df_RSFSDP = get_stlouisfed_data('RSFSDp')
df_RSGASS = get_stlouisfed_data('RSGASs')
df_RSMVPD = get_stlouisfed_data('RSMVPd')
df_RSBMGESD = get_stlouisfed_data('RSBMGESD')
df_RSFHFS = get_stlouisfed_data('RSFHFS')
df_RSCCAS = get_stlouisfed_data('RSCCAS')
df_RSMSR = get_stlouisfed_data('RSMSR')
df_RSEAS = get_stlouisfed_data('RSEAS')

#Combine all these data frames into a single data frame based on the DATE field

df = pd.merge(df_RSAFS,df_RSFSXMV,"right")
df = pd.merge(df,df_MARTSSM44W72USS,"left")
df = pd.merge(df,df_RSDBS,"left")
df = pd.merge(df,df_RSNSR,"left")
df = pd.merge(df,df_RSHPCS,"left")
df = pd.merge(df,df_RSSGHBMS,"left")
df = pd.merge(df,df_RSGMS,"left")
df = pd.merge(df,df_RSFSDP,"left")
df = pd.merge(df,df_RSGASS,"left")
df = pd.merge(df,df_RSMVPD,"left")
df = pd.merge(df,df_RSBMGESD,"left")
df = pd.merge(df,df_RSFHFS,"left")
df = pd.merge(df,df_RSCCAS,"left")
df = pd.merge(df,df_RSMSR,"left")
df = pd.merge(df,df_RSEAS,"left")

#Make all column names uppercase
df.columns = map(str.upper, df.columns)

df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

# Check for difference between original and new lists
#print(util_check_diff_list(df_original.columns.tolist(), df.columns.tolist()))

df_updated = combine_df(df_original, df)

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

#LEGACY Write to a csv file in the correct directory
#write_to_directory(df,'011_Lagging_Indicator_Retail_Sales.csv')

print("Done!")
