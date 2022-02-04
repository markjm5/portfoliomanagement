import requests
import os.path
import csv
import pandas as pd
from common import get_stlouisfed_data, convert_excelsheet_to_dataframe, write_dataframe_to_excel
from common import combine_df_on_index

excel_file_path = '/Trading_Excel_Files/02_Interest_Rates_FX/013_Yield_Curve.xlsm'
sheet_name = 'Database'

df_DTB3 = get_stlouisfed_data('DTB3')
df_DGS10 = get_stlouisfed_data('DGS10')

#Combine all these data frames into a single data frame based on the DATE field

df = combine_df_on_index(df_DTB3,df_DGS10,"DATE")

# Get Original Sheet and store it in a dataframe
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

df_updated = combine_df_on_index(df_original, df, 'DATE')

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

print("Done!")
