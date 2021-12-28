import requests
import os.path
import csv
import pandas as pd
from common import get_stlouisfed_data, convert_excelsheet_to_dataframe, write_dataframe_to_excel, combine_df, write_to_directory, merge_two_df

excel_file_path = '/Trading_Excel_Files/01_Lagging_Coincident_Indicators/002_Lagging_Indicator_US_GDP.xlsm'
sheet_name = 'Database'

df_DPCCRV1Q225SBEA = get_stlouisfed_data('DPCCRV1Q225SBEA')
df_EXPGSC1 = get_stlouisfed_data('EXPGSC1')
df_GCEC1 = get_stlouisfed_data('GCEC1')
df_GDPC1 = get_stlouisfed_data('GDPC1')
df_GDPCTPI = get_stlouisfed_data('GDPCTPI')
df_GPDIC1 = get_stlouisfed_data('GPDIC1')
df_IMPGSC1 = get_stlouisfed_data('IMPGSC1')
df_JCXFE = get_stlouisfed_data('JCXFE')
df_PCECC96 = get_stlouisfed_data('PCECC96')

#Combine all these data frames into a single data frame based on the DATE field

df = pd.merge(df_DPCCRV1Q225SBEA,df_EXPGSC1,"right")
df = pd.merge(df,df_GCEC1,"left")
df = pd.merge(df,df_GDPC1,"left")
df = pd.merge(df,df_GDPCTPI,"left")
df = pd.merge(df,df_GPDIC1,"left")
df = pd.merge(df,df_IMPGSC1,"left")
df = pd.merge(df,df_JCXFE,"left")
df = pd.merge(df,df_PCECC96,"left")

#LEGACY: Write to a csv file in the correct directory
#write_to_directory(df,'002_Lagging_Indicator_US_GDP.csv')

# Rather than writing to directory, update and save the sheet
# Get Original Sheet and store it in a dataframe
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)
#df_updated = append_new_rows_to_df(df_original, df, 'DATE')

df_updated = combine_df(df_original, df)

"""
df_updated = df_original
for index, row in df.iterrows():
    if(row['DATE'] not in df_original.values):
        df_new_row = df[df['DATE']==row['DATE']]
        df_updated = pd.concat([df_updated, df_new_row], ignore_index=False)
"""

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

print("Done!")
