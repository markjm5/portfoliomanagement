import pandas as pd
from datetime import datetime as dt
from datetime import date
from bs4 import BeautifulSoup
from requests.models import parse_header_links
from common import get_oecd_data, convert_excelsheet_to_dataframe, write_dataframe_to_excel
from common import combine_df_on_index, get_stlouisfed_data, get_page

excel_file_path = '/Trading_Excel_Files/02_Interest_Rates_FX/014_US_Global_Money_Supply.xlsm'

#Scrape this table and get latest ADP number
def scrape_money_supply_table(url):
  #Scrape GDP Table from Trading Economics
  page = get_page(url)

  soup = BeautifulSoup(page.content, 'html.parser')

  table = soup.find('table')
  table_rows = table.find_all('tr')
  table_rows_header = table.find_all('tr')[0].find_all('th')
  df = pd.DataFrame()

  index = 0

  for header in table_rows_header:
    df.insert(index,str(header.text).strip(),[],True)
    index+=1

  #Insert New Row
  for tr in table_rows:
    temp_row = []
    index = 0
    td = tr.find_all('td')

    if(td): 
      for obs in td:
        if(index == 3):
          dt_date = dt.strptime(str(obs.text),'%b/%y')
          text = dt_date.strftime('%-d/%-m/%Y')
        else:
          text = str(obs.text).strip()
        temp_row.append(text)        
        index += 1

      df.loc[len(df.index)] = temp_row

  #Format datatypes
  df['Last'] = pd.to_numeric(df['Last'])
  df['Previous'] = pd.to_numeric(df['Previous'])
  df['Reference'] = pd.to_datetime(df['Reference'],format='%d/%m/%Y')

  return df

#################################################
# Get US M1, M2 Monthly Data from St Louis Fred #
#################################################

sheet_name = 'DB Money Supply'

df_M1REAL = get_stlouisfed_data('M1REAL')
df_M2REAL = get_stlouisfed_data('M2REAL')
df_M1 = get_stlouisfed_data('M1')
df_M2SL = get_stlouisfed_data('M2SL')
df_M2V = get_stlouisfed_data('M2V')

#Combine all these data frames into a single data frame based on the DATE field
df = combine_df_on_index(df_M1REAL, df_M2REAL, 'DATE')
df = combine_df_on_index(df_M1, df, 'DATE')
df = combine_df_on_index(df_M2SL, df, 'DATE')
df = combine_df_on_index(df_M2V, df, 'DATE')

#Get original data from sheet
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

#Combine new data with original data
df_updated = combine_df_on_index(df_original, df, 'DATE')

# get a list of columns
cols = list(df_updated)
# move the column to head of list using index, pop and insert
cols.insert(0, cols.pop(cols.index('DATE')))
cols.insert(1, cols.pop(cols.index('M1REAL')))
cols.insert(2, cols.pop(cols.index('M2REAL')))
cols.insert(3, cols.pop(cols.index('M1')))
cols.insert(4, cols.pop(cols.index('M2SL')))

# reorder
df_updated = df_updated[cols]

#Write to excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

########################################################
# Get Monthly Money Supply Data from Trading Economics #
########################################################

sheet_name = 'DB Trading Economics'

# Scrape Money Supply Table from Trading Economics Site
df_te_money_supply = scrape_money_supply_table("https://tradingeconomics.com/country-list/money-supply-m2?continent=g20")

#Rename Reference field to Month
df_te_money_supply = df_te_money_supply.rename(columns={"Reference": "Month"})

#Rename Unit Field to Currency
df_te_money_supply = df_te_money_supply.rename(columns={"Unit": "Currency"})

#Get original data from sheet
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

#Combine new data with original data
df_updated = combine_df_on_index(df_original, df_te_money_supply, 'Country')

# get a list of columns
cols = list(df_updated)
# move the column to head of list using index, pop and insert
cols.insert(0, cols.pop(cols.index('Country')))
cols.insert(1, cols.pop(cols.index('Last')))
cols.insert(2, cols.pop(cols.index('Month')))
cols.insert(3, cols.pop(cols.index('Previous')))
cols.insert(4, cols.pop(cols.index('Currency')))

# Reorder Fields
df_updated = df_updated[cols]

#Write to excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, -1)

##########################################
# Get Global Money Supply Data from OECD #
##########################################

#TODO: Get data from correct OECD time series
#TODO: Update excel sheet to remove unnecessary columns
# https://stats.oecd.org/restsdmx/sdmx.ashx/GetData/MEI_FIN/MABM.AUS+CAN+CHL+COL+CZE+DNK+HUN+ISL+ISR+JPN+KOR+MEX+NZL+NOR+POL+SWE+CHE+TUR+GBR+USA+EA19+OECD+NMEC+BRA+CHN+IND+IDN+RUS+ZAF.M/all?startTime=2018&endTime=2021

country = ['AUS','BRA','CAN','CHE','CHL','CHN','COL','CRI','CZE','DNK','EA19','GBR','HUN','IDN','IND','ISL','ISR','JPN','KOR','MEX','NOR','OECD','OECDE','POL','RUS','SWE','TUR','USA','ZAF','NZL']
subject = ['MABM']
measure = []
frequency = 'M'
startDate = '1951-Q1'

todays_date = date.today()
endDate = '%s-Q4' % (todays_date.year)

df_money_supply = get_oecd_data('MEI_FIN', [country, subject, measure, [frequency]], {'startTime': startDate, 'endTime': endDate, 'dimensionAtObservation': 'AllDimensions','filename': '014_US_Global_Money_Supply.xml'})
df_money_supply = df_money_supply.drop(columns='MTH', axis=1)

sheet_name = 'Data'
df_original_money_supply = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

# Check for difference between original and new lists
#util_check_diff_list(df_money_supply.columns.tolist(), df_original_money_supply.columns.tolist())
#util_check_diff_list(df_original_money_supply.columns.tolist(),df_money_supply.columns.tolist())

#df_updated_money_supply = combine_df(df_original_money_supply, df_money_supply)
df_updated_money_supply = combine_df_on_index(df_original_money_supply, df_money_supply, 'DATE')

# get a list of columns
cols = list(df_updated_money_supply)
# move the column to head of list using index, pop and insert
cols.insert(0, cols.pop(cols.index('DATE')))

# reorder
df_updated_money_supply = df_updated_money_supply[cols]

# format date
df_updated_money_supply['DATE'] = pd.to_datetime(df_updated_money_supply['DATE'],format='%Y-%m-%d')

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated_money_supply, False, 0)

print("Done!")
