import requests
import os.path
import csv
import pandas as pd
#import openpyxl
from common import get_stlouisfed_data, write_to_directory, convert_excelsheet_to_dataframe, write_dataframe_to_excel

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

#Write to a csv file in the correct directory
write_to_directory(df,'002_Lagging_Indicator_US_GDP.csv')

# Rather than writing to directory, update and save the sheet
# Get Original Sheet and store it in a dataframe
df_original = convert_excelsheet_to_dataframe('/trading_excel_files/01_lagging_coincident_indicators/002_lagging_indicator_us_gdp.xlsm', 'Database')

#TODO: update df_original with the dataframe with new values 


#TODO: Write back to the excel sheet
#write_dataframe_to_excel('/trading_excel_files/01_lagging_coincident_indicators/002_lagging_indicator_us_gdp.xlsm', df_original)

print("Done!")
