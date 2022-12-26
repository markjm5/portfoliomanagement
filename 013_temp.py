import os
import csv
import pandas as pd
from datetime import date
from bs4 import BeautifulSoup
from common import get_oecd_data, get_invest_data,convert_excelsheet_to_dataframe, write_dataframe_to_excel
from common import combine_df_on_index, get_yf_historical_stock_data, get_page, get_page_selenium, convert_html_table_to_df

excel_file_path = '/Trading_Excel_Files/02_Interest_Rates_FX/013_Interest_Rates.xlsm'

def read_csv(excel_file_path):

    filepath = os.getcwd()
    excel_file_path = filepath + excel_file_path.replace("/","\\")

    df = pd.read_csv(excel_file_path)

    return df


def get_invest_data_manual_scrape(country_list, bond_year):

  data = {'DATE': []}

  # Convert the dictionary into DataFrame
  df_invest_data = pd.DataFrame(data)

  for country in country_list:
    print("Getting %s-y data for: %s" % (bond_year,country))
    file_path = '/trading_excel_files/reference/%sy/%s-%s-Year.csv' % (bond_year,country,bond_year)
    
    df_country_rates = read_csv(file_path)

    try:
        df_country_rates = df_country_rates.drop(['Open', 'High', 'Low', 'Change %'], axis=1)
        df_country_rates['Date'] = pd.to_datetime(df_country_rates['Date'],format='%b %d, %Y')
        df_country_rates = df_country_rates.rename(columns={"Date": "DATE","Price": country})
        df_country_rates[country] = pd.to_numeric(df_country_rates[country])
        df_invest_data = combine_df_on_index(df_invest_data, df_country_rates, 'DATE')

    except KeyError as e:
        print("======================================%s DOES NOT EXIST=======================================" % country)
        print("======================================%s DOES NOT EXIST=======================================" % country)
        print("======================================%s DOES NOT EXIST=======================================" % country)

  return df_invest_data.drop_duplicates()


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


print("Done!")
