import requests
import os.path
import csv
import pandas as pd
from common import get_stlouisfed_data, convert_excelsheet_to_dataframe, write_dataframe_to_excel, combine_df_on_index, write_to_directory

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

df = combine_df_on_index(df_RSAFS,df_RSFSXMV,"DATE")
df = combine_df_on_index(df_MARTSSM44W72USS,df,"DATE")
df = combine_df_on_index(df_RSDBS,df,"DATE")
df = combine_df_on_index(df_RSNSR,df,"DATE")
df = combine_df_on_index(df_RSHPCS,df,"DATE")
df = combine_df_on_index(df_RSSGHBMS,df,"DATE")
df = combine_df_on_index(df_RSGMS,df,"DATE")
df = combine_df_on_index(df_RSFSDP,df,"DATE")
df = combine_df_on_index(df_RSGASS,df,"DATE")
df = combine_df_on_index(df_RSMVPD,df,"DATE")
df = combine_df_on_index(df_RSBMGESD,df,"DATE")
df = combine_df_on_index(df_RSFHFS,df,"DATE")
df = combine_df_on_index(df_RSCCAS,df,"DATE")
df = combine_df_on_index(df_RSMSR,df,"DATE")
df = combine_df_on_index(df_RSEAS,df,"DATE")

#Make all column names uppercase
df.columns = map(str.upper, df.columns)

df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

# Check for difference between original and new lists
#print(util_check_diff_list(df_original.columns.tolist(), df.columns.tolist()))

df_updated = combine_df_on_index(df_original, df, 'DATE')

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

print("Done!")
