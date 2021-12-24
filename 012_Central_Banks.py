import requests
import os.path
import csv
import pandas as pd
from common import get_stlouisfed_data, convert_excelsheet_to_dataframe, write_dataframe_to_excel, combine_df, write_to_directory, util_check_diff_list

excel_file_path = '/Trading_Excel_Files/02_Interest_Rates_FX/012_Central_Banks.xlsm'

#######################################
#   Get St Louis Fed Data for Rates   #
#######################################

sheet_name = 'Database Rates'

df_INTDSRUSM193N = get_stlouisfed_data('INTDSRUSM193N')
df_FEDFUNDS = get_stlouisfed_data('FEDFUNDS')
df_M2SL = get_stlouisfed_data('M2SL')

#Combine all these data frames into a single data frame based on the DATE field

df = pd.merge(df_INTDSRUSM193N,df_FEDFUNDS,"left")
df = pd.merge(df,df_M2SL,"left")

df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

# Check for difference between original and new lists
#print(util_check_diff_list(df_original.columns.tolist(), df.columns.tolist()))

df_updated = combine_df(df_original, df)

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

######################################
#   Get St Louis Fed Balance Sheet   #
######################################

sheet_name = 'Database FED'

df_WALCL = get_stlouisfed_data('WALCL')

df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

df_updated = combine_df(df_original, df_WALCL)

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

######################################
#   Get St Louis BOJ Balance Sheet   #
######################################

sheet_name = 'Database BOJ'

df_JPNASSETS = get_stlouisfed_data('JPNASSETS')

df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

df_updated = combine_df(df_original, df_JPNASSETS)

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)


######################################
#   Get St Louis ECB Balance Sheet   #
######################################

sheet_name = 'Database ECB'

df_ECBASSETSW = get_stlouisfed_data('ECBASSETSW')

df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

df_updated = combine_df(df_original, df_ECBASSETSW)

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

print("Done!")
