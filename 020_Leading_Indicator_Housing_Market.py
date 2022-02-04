import requests
import os.path
import csv
import pandas as pd
from common import get_stlouisfed_data, convert_excelsheet_to_dataframe, write_dataframe_to_excel
from common import combine_df_on_index

excel_file_path = '/Trading_Excel_Files/03_Leading_Indicators/020_Leading_Indicator_Housing_Market.xlsm'
sheet_name = 'Database New'

df_PERMIT = get_stlouisfed_data('PERMIT')
df_HOUST = get_stlouisfed_data('HOUST')
df_COMPUTSA = get_stlouisfed_data('COMPUTSA')

#Combine all these data frames into a single data frame based on the DATE field

df = combine_df_on_index(df_PERMIT,df_HOUST,"DATE")
df = combine_df_on_index(df_COMPUTSA,df,"DATE")

# Get Original Sheet and store it in a dataframe
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

df_updated = combine_df_on_index(df_original, df, 'DATE')

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

print("Done!")
