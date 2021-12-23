import pandas as pd
import wbgapi as wb
import requests
import re
import calendar
from datetime import datetime as dt
from datetime import date
from bs4 import BeautifulSoup
from requests.models import parse_header_links
from common import get_oecd_data, convert_excelsheet_to_dataframe, write_dataframe_to_excel, combine_df, write_to_directory, util_check_diff_list, scrape_world_gdp_table

excel_file_path = '/Trading_Excel_Files/01_Lagging_Coincident_Indicators/010_Lagging_Indicator_World_Industrial_Production.xlsm'

"""
def get_wb_capital_investment(country_list):
  #Scrape GDP Table from https://www.theglobaleconomy.com/rankings/Capital_investment/
  #World Bank Data API: https://pypi.org/project/wbgapi/
  #https://data.worldbank.org/indicator/NE.GDI.TOTL.ZS?end=2020&start=1960&view=chart

  wb_df = wb.data.DataFrame(['NE.GDI.TOTL.ZS'], country_list, mrv=1) # most recent 5 years

  page = requests.get(url=url)

  if(page.status_code == 200):
    soup = BeautifulSoup(page.content, 'html.parser')

    #Need to scrape div for capital investment list countries and numbers.
    value = soup.find('input', {'id': 'export_data'}).get('value')
    countries = value.split('=')[4].split('&')[0]
    numbers = value.split('=')[3].split('&')[0]

    #Need to put countries and numbers into a pandas dataframe.
    dataframe_values = {'COUNTRY': countries.split('|'), 'CAPITAL_INVESTMENT': numbers.split('|')}
    df = pd.DataFrame(data=dataframe_values)
  else:
    print(page.status_code)

  return wb_df
  """

def scrape_table_world_production(url):
  #Scrape GDP Table from https://tradingeconomics.com/country-list/industrial-production?continent=world

  page = requests.get(url=url)
  soup = BeautifulSoup(page.content, 'html.parser')

  #TODO: Need to scrape table for world production countries and numbers.
  table = soup.find('table')
  #import pdb; pdb.set_trace()
  table_rows = table.find_all('tr', recursive=False)
  table_rows_header = table.find_all('tr')[0].find_all('th')
  df = pd.DataFrame()
  index = 0
  for header in table_rows_header:
    if(index == 3):
      df.insert(3,"Month",[],True)
    else:
      df.insert(index,str(header.text).strip(),[],True)
    index+=1

  #Get rows of data.
  for tr in table_rows:
    temp_row = []
    #first_col = True
    index = 0
    td = tr.find_all('td')
    for obs in td:
      if(index == 3):
        dt_date = dt.strptime(str(obs.text),'%b/%y')
        text = dt_date.strftime('%b-%y')
      else:
        text = str(obs.text).strip()
      temp_row.append(text)        
      index += 1

    df.loc[len(df.index)] = temp_row

  return df

def scrape_table_china_production():

  url_ip_yoy = 'https://tradingeconomics.com/china/industrial-production'
  url_caixin_pmi = 'https://tradingeconomics.com/china/manufacturing-pmi'

  page = requests.get(url=url_ip_yoy)
  soup = BeautifulSoup(page.content, 'html.parser')

  #Need to scrape table for china production and numbers.
  table = soup.find('table')
  table_rows = table.find_all('tr', recursive=False)
  table_rows_header = table.find_all('tr')[0].find_all('th')
  df_ip_yoy = pd.DataFrame()

  index = 0
  for header in table_rows_header:
    if(index == 0):
      df_ip_yoy.insert(0,"Calendar",[],True)
    else:
      df_ip_yoy.insert(index,str(header.text).strip(),[],True)
    index+=1

  #Get rows of data.
  for tr in table_rows:
    temp_row = []
    index = 0
    td = tr.find_all('td')
    for obs in td:
      if(str(obs.text).strip() == 'Industrial Production YoY'):
        pass
      else:
        text = str(obs.text).strip()
        temp_row.append(text)        
        index += 1
    df_ip_yoy.loc[len(df_ip_yoy.index)] = temp_row

  df_ip_yoy_last = df_ip_yoy.query("Actual !=''").iloc[-1:]

  #TODO: Add additional column to df with China PMI Index
  page = requests.get(url=url_caixin_pmi)
  soup = BeautifulSoup(page.content, 'html.parser')

  table = soup.find('table')
  table_rows = table.find_all('tr', recursive=False)
  table_rows_header = table.find_all('tr')[0].find_all('th')
  df_caixin_pmi = pd.DataFrame()

  index = 0
  for header in table_rows_header:
    df_caixin_pmi.insert(index,str(header.text).strip(),[],True)
    index += 1

  for tr in table_rows:
    temp_row = []
    index = 0
    td = tr.find_all('td')
    for obs in td:
      text = str(obs.text).strip()
      temp_row.append(text)        
      index += 1
    df_caixin_pmi.loc[len(df_caixin_pmi.index)] = temp_row
    df_caixin_manufacturing_pmi = df_caixin_pmi[df_caixin_pmi['Related'].isin(['Manufacturing PMI'])]

  #Combine IP YoY df with Caixin PMI df
  df_combined = pd.concat([df_caixin_manufacturing_pmi,df_ip_yoy_last], axis=1)

  #Print dataframe with Date, YoY, HSBC China PMI headers
  year = dt.strptime(df_combined.iloc[0]['Calendar'],'%Y-%m-%d').year
  month = dt.strptime(df_combined.iloc[0]['Calendar'],'%Y-%m-%d').month

  #get last day of month using the year and month
  day = calendar.monthrange(dt.strptime(df_combined.iloc[0]['Calendar'],'%Y-%m-%d').year,dt.strptime(df_combined.iloc[0]['Calendar'],'%Y-%m-%d').month)[1]
  date_str = "%s/%s/%s" % (day,month,year)

  df_selections = df_combined[["Actual", "Last"]]
  df_final = df_selections.rename(columns={"Actual": "YoY", "Last": "HSBC China PMI"})

  #Add date_str to df_final
  df_final.insert(0, "Date", date_str, True)

  df_final['Date'] = pd.to_datetime(df_final['Date'],format='%d/%m/%Y')
  df_final['HSBC China PMI'] = pd.to_numeric(df_final['HSBC China PMI'])
  df_final['YoY'] = float(df_final['YoY'].str.strip('%'))/100

  return df_final

#####################################
#   Get Capital Investment Data     #
#####################################

sheet_name = 'Data World GDP'

# Use worldbank API to get capital investment data
#Get Capital Investment Data for the following countries
country_list = ['USA','CHN','CEB','EUN','JPN','DEU','GBR','FRA','IND','ITA','CAN','KOR','RUS','BRA','AUS','ESP','MEX','IDN','NLD']

df_capital_investment =  wb.data.DataFrame(['NE.GDI.TOTL.ZS'], country_list, mrv=1) # most recent 1 year

df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

# Add new index
df_capital_investment.reset_index(level=0, inplace=True)

# Rename columns in df_capital_investment
df_capital_investment = df_capital_investment.rename(columns={'economy': 'Code', 'NE.GDI.TOTL.ZS': 'Investment_Percentage'})

# Combine df_original with df_capital_investment
df_updated = df_original.merge(df_capital_investment, on='Code')

df_updated = df_updated.drop(['Investment_Percentage_x'], axis=1)
df_updated = df_updated.rename(columns={'Investment_Percentage_y': 'Investment_Percentage'})

#LEGACY: Write to a csv file in the correct directory
#write_to_directory(df_capital_investment,'010_Lagging_Indicator_Capital_Investment.csv')

df_world_gdp = scrape_world_gdp_table("https://tradingeconomics.com/matrix")

#Drop unnecessary columns
col_drop = ['GDP YoY','GDP QoQ','Interest rate','Inflation rate','Jobless rate','Gov. Budget','Debt/GDP','Current Account','Currency','Population']
df_world_gdp = df_world_gdp.drop(col_drop, axis=1)

#Fix datatypes of df_world_gdp
df_world_gdp['GDP'] = pd.to_numeric(df_world_gdp['GDP'])

df_updated = pd.merge(df_updated,df_world_gdp,"left", on='Country')

#Drop unnecessary columns
df_updated_1 = df_updated.drop('GDP_x', axis=1)
df_updated_2 = df_updated_1.rename(columns={'GDP_y': 'GDP'})

column_names = ["Code", "Country", "GDP", "Investment_Percentage"]

df_updated_3 = df_updated_2.reindex(columns=column_names)

write_dataframe_to_excel(excel_file_path, sheet_name, df_updated_3, False, -1)

##################################################
#   Get World IP Data from Trading Economics     #
##################################################

sheet_name = 'World Production data'

#Get World Production Data
df_world_production = scrape_table_world_production("https://tradingeconomics.com/country-list/industrial-production?continent=world")

df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False, 'Country')

#Fix datatypes of df_world_gdp
df_world_production['Last'] = pd.to_numeric(df_world_production['Last'])
df_world_production['Previous'] = pd.to_numeric(df_world_production['Previous'])

#Make the Country col the index so that we can combine
#df_original.set_index('Country', inplace=True)
df_world_production.set_index('Country', inplace=True)

df_updated = combine_df(df_original, df_world_production)

write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, True, -1)

#LEGACY Write to a csv file in the correct directory
#write_to_directory(df_world_production,'010_Lagging_Indicator_World_Production.csv')

##################################################
#   Get China IP Data from Trading Economics     #
##################################################

sheet_name = 'China Production data'

#Get China Production Data
df_china_production = scrape_table_china_production()

df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

# check if row does not already exist, and if it doesnt append it to the end. 
if(not df_china_production['Date'].values in df_original['Date'].values):
  df_updated = df_original.append(df_china_production, ignore_index=True)
else:
  df_updated = df_original

write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

#LEGACY Write to a csv file in the correct directory
#write_to_directory(df_china_production,'010_Lagging_Indicator_China_Production.csv')

#####################################
#   Get World IP Data from OECD     #
#####################################

sheet_name = 'Database'

# Get OECD Data Using API: https://stackoverflow.com/questions/40565871/read-data-from-oecd-api-into-python-and-pandas
#Get World Industrial Production
#https://stats.oecd.org/restsdmx/sdmx.ashx/GetData/KEI/PRINTO01.AUS+AUT+BEL+USA.ST.M/all?startTime=1919-01&endTime=2021-11
country = ['AUS','AUT','BEL','CAN','CHL','CZE','DEU','DNK','ESP','EST','FIN','FRA','GBR','GRC','HUN','IRL','ISL','ISR','ITA','JPN','KOR','LUX','LVA','MEX','NLD','NOR','OECD','POL','PRT','SVK','SVN','SWE','NZL','USA','EA19','G-7','CHE','IND','ZAF','RUS','TUR','BRA','ARG']
subject = ['PRINTO01']
measure = ['ST']
frequency = 'M'
startDate = '1919-Q1'

todays_date = date.today()
endDate = '%s-Q4' % (todays_date.year)

df_world_industrial_production = get_oecd_data('KEI', [country, subject, measure, [frequency]], {'startTime': startDate, 'endTime': endDate, 'dimensionAtObservation': 'AllDimensions','filename': '010_World_Industrial_Production.xml'})

df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)
df_world_industrial_production = df_world_industrial_production.drop('MTH', 1)

# Check for difference between original and new lists
#print(util_check_diff_list(df_world_industrial_production.columns.tolist(), df_original.columns.tolist()))

df_updated_world_industrial_production = combine_df(df_original, df_world_industrial_production)

# get a list of columns
cols = list(df_updated_world_industrial_production)
# move the column to head of list using index, pop and insert
cols.insert(0, cols.pop(cols.index('DATE')))

# reorder
df_updated_world_industrial_production = df_updated_world_industrial_production[cols]

# format date
df_updated_world_industrial_production['DATE'] = pd.to_datetime(df_updated_world_industrial_production['DATE'],format='%d/%m/%Y')

write_dataframe_to_excel(excel_file_path, sheet_name, df_updated_world_industrial_production, False, 0)

#Write to a csv file in the correct directory
#write_to_directory(df_world_industrial_production,'010_Lagging_Indicator_World_Indistrial_Production.csv')

print("Done!")
