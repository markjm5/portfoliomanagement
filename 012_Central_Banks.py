import requests
import os.path
import csv
import pandas as pd
from datetime import date
from common import get_stlouisfed_data, convert_excelsheet_to_dataframe, write_dataframe_to_excel
from common import combine_df_on_index, get_yf_data
from common import append_two_df

excel_file_path = '/Trading_Excel_Files/02_Interest_Rates_FX/012_Central_Banks.xlsm'

#######################################
#   Get St Louis Fed Data for Rates   #
#######################################

sheet_name = 'Database Rates'

df_INTDSRUSM193N = get_stlouisfed_data('INTDSRUSM193N')
df_FEDFUNDS = get_stlouisfed_data('FEDFUNDS')
df_M2SL = get_stlouisfed_data('M2SL')

#Combine all these data frames into a single data frame based on the DATE field

#TODO: write a merge function that always ensures latest data is not cut out when 
#merging right or merging left

df = append_two_df(df_INTDSRUSM193N, df_FEDFUNDS)
df = append_two_df(df,df_M2SL)

#df = pd.merge(df_INTDSRUSM193N,df_FEDFUNDS,"left")
#df = pd.merge(df,df_M2SL,"left")

df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

# Check for difference between original and new lists
#print(util_check_diff_list(df_original.columns.tolist(), df.columns.tolist()))

df_updated = combine_df_on_index(df_original, df, 'DATE')

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

######################################
#   Get St Louis Fed Balance Sheet   #
######################################

sheet_name = 'Database FED'

df_WALCL = get_stlouisfed_data('WALCL')

df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

df_updated = combine_df_on_index(df_original, df_WALCL,'DATE')

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

######################################
#   Get St Louis BOJ Balance Sheet   #
######################################

sheet_name = 'Database BOJ'

#Get Dataframes
df_JPNASSETS = get_stlouisfed_data('JPNASSETS')
df_JPNNGDP = get_stlouisfed_data('JPNNGDP')
#df_JPNNGDP = get_stlouisfed_data('JPNRGDPEXP')

todays_date = date.today()
date_str = "%s-%s-%s" % (todays_date.year, todays_date.month, todays_date.day)

# Get Nikkei 225 close monthly intervals
df_NIKKEI = get_yf_data("^N225", "1mo", "1998-04-01", date_str)

# Format JPNNGDP series so that the DATE is monthly rather than quarterly
df_JPNNGDP['JPNNGDP'] = df_JPNNGDP['JPNNGDP'] * 10
#df['date'] = pd.to_datetime(df['date'])
#df = df.sort_values(by=['date'], ascending=[True])
df_JPNNGDP.set_index('DATE', inplace=True)
df_JPNNGDP = df_JPNNGDP.asfreq('MS', method='bfill').reset_index() #.to_period('M').reset_index()

# Combine df_JPNNGDP with df_JPNASSETS and df_NIKKEI
df = combine_df_on_index(df_JPNASSETS,df_JPNNGDP,"DATE")
df = combine_df_on_index(df_NIKKEI,df,"DATE")

# Clean up columns
df = df.drop(['Open', 'High', 'Low', 'Volume'], axis=1)
df = df.rename(columns={"Close": "N225"})

#Get Original Dataframe
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

df_updated = combine_df_on_index(df_original, df, "DATE")

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

######################################
#   Get St Louis ECB Balance Sheet   #
######################################

sheet_name = 'Database ECB'

df_ECBASSETSW = get_stlouisfed_data('ECBASSETSW')

df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

df_updated = combine_df_on_index(df_original, df_ECBASSETSW,"DATE")

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

print("Done!")
