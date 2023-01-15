import os
import csv
import pandas as pd
from datetime import date
from bs4 import BeautifulSoup
from common import get_oecd_data, get_invest_data, write_dataframe_to_excel, get_invest_data_manual_scrape, convert_excelsheet_to_dataframe
from common import combine_df_on_index, get_yf_historical_stock_data, get_page, get_page_selenium, convert_html_table_to_df

def convert_excelsheet_to_dataframe_temp(excel_file_path,sheet_name,date_exists=False, index_col=None, date_format='%d/%m/%Y'):

  filepath = os.getcwd()
  excel_file_path = filepath + excel_file_path.replace("/","\\")

  df = pd.read_excel(excel_file_path, sheet_name=sheet_name, engine='openpyxl')

  # format date
  df['DATE'] = pd.to_datetime(df['DATE'],format='%b %d, %Y')

  df[sheet_name] = pd.to_numeric(df[sheet_name])

  return df


excel_file_path = '/Trading_Excel_Files/02_Interest_Rates_FX/013_Interest_Rates.xlsm'

sheet_names = ['mexico','norway','hungary','greece','indonesia']

data = {'DATE': []}

# Convert the dictionary into DataFrame
df_invest_data = pd.DataFrame(data)

for sheet_name in sheet_names:

  country_df = convert_excelsheet_to_dataframe_temp(excel_file_path, sheet_name, True, None,'%d/%m/%Y')
  df_invest_data = combine_df_on_index(df_invest_data, country_df, 'DATE')

#TODO: Combine with master country DF
#TODO: Get Sheet 3y5y, and combine master country DF with the sheet
#TODO: Write to excel

sheet_name = 'DB 3y5y' #TODO: Make sure excel file 013 has updated sheet name

df_original_invest_3y5y = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True, None,'%d/%m/%Y')
df_updated_invest_3y5y = combine_df_on_index(df_original_invest_3y5y, df_invest_data, 'DATE')

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated_invest_3y5y, False, 0)

"""

############################################
# Get 10y database Data from Investing.com #
############################################

sheet_name = 'DB 10y'
df_original_invest_10y = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True, None,'%d/%m/%Y')


country_list = ['u.s.','canada','brazil','mexico','germany','france','italy','spain','portugal','netherlands','austria','greece','norway','switzerland','uk','russia','turkey','poland','hungary','czech-republic','south-africa','japan','australia','new-zealand','singapore','china','hong-kong','india','indonesia','south-korea','philippines','thailand','vietnam']

df_invest_10y = get_invest_data_manual_scrape(country_list,'10')

df_invest_10y = df_invest_10y.rename(columns={"hong-kong": "hong kong", "south-korea": "south korea", "czech-republic": "czech republic", "south-africa": "south africa", "new-zealand": "new zealand", "uk":"u.k."})

df_updated_invest_10y = combine_df_on_index(df_original_invest_10y, df_invest_10y, 'DATE')

# get a list of columns
cols = list(df_updated_invest_10y)
# move the column to head of list using index, pop and insert
cols.insert(0, cols.pop(cols.index('DATE')))

# reorder
df_updated_invest_10y = df_updated_invest_10y[cols]

# format date
#df_updated_invest_10y['DATE'] = pd.to_datetime(df_updated_invest_10y['DATE'],format='%b %d, %Y')

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated_invest_10y, False, 0)


###########################################
# Get 2y database Data from Investing.com #
###########################################

sheet_name = 'DB 2y' 
df_original_invest_2y = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True, None,'%d/%m/%Y')

country_list = ['u.s.','canada','brazil','germany','france','italy','spain','portugal','netherlands','austria','switzerland', 'uk','russia','turkey','poland','czech-republic','south-africa','japan','australia','new-zealand','singapore','china','hong-kong','india','south-korea','philippines','thailand','vietnam']

df_invest_2y = get_invest_data_manual_scrape(country_list,'2')
df_invest_2y = df_invest_2y.rename(columns={"hong-kong": "hong kong", "south-korea": "south korea", 
            "south-africa": "south africa", "new-zealand": "new zealand", "czech-republic": "czech republic", "uk":"u.k."})

df_updated_invest_2y = combine_df_on_index(df_original_invest_2y, df_invest_2y, 'DATE')

# get a list of columns
cols = list(df_updated_invest_2y)
# move the column to head of list using index, pop and insert
cols.insert(0, cols.pop(cols.index('DATE')))

# reorder
df_updated_invest_2y = df_updated_invest_2y[cols]

# format date
#df_updated_invest_2y['DATE'] = pd.to_datetime(df_updated_invest_2y['DATE'],format='%b %d, %Y')

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated_invest_2y, False, 0)
"""

print("Done!")
