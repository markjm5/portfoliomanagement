import pandas as pd
import wbgapi as wb
import requests
import re
import calendar
from datetime import datetime as dt
from datetime import date
from bs4 import BeautifulSoup
from common import get_oecd_data, convert_excelsheet_to_dataframe, write_dataframe_to_excel, combine_df_on_index, scrape_world_gdp_table
from common import convert_html_table_to_df

excel_file_path = '/Trading_Excel_Files/01_Lagging_Coincident_Indicators/010_Lagging_Indicator_World_Industrial_Production.xlsm'

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

def scrape_table_china_caixin_pmi():
  #currentYear = dt.now().strftime('%Y')
  url_caixin_pmi = 'https://www.investing.com/economic-calendar/chinese-caixin-manufacturing-pmi-753'

  # When website blocks your request, simulate browser request: https://stackoverflow.com/questions/56506210/web-scraping-with-python-problem-with-beautifulsoup
  header={'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.0 Safari/537.36'}
  page = requests.get(url=url_caixin_pmi,headers=header)
  soup = BeautifulSoup(page.content, 'html.parser')

  table = soup.find('table') 
  #table_rows = table.find_all('tr', recursive=True)
  #table_rows_header = table.find_all('tr')[0].find_all('th')

  df_caixin_pmi = convert_html_table_to_df(table, False)
  
  #Date Transformations
  df_caixin_pmi['Month_Day'], df_caixin_pmi['Year_Temp'] = df_caixin_pmi['Release Date'].str.split(',', n=1, expand=False).str
  df_caixin_pmi['Year'],df_caixin_pmi['Prev_Month'] = df_caixin_pmi['Year_Temp'].str.split('(', n=1, expand=False).str
  df_caixin_pmi['Prev_Month'] = df_caixin_pmi['Prev_Month'].map(lambda x: x.rstrip(')'))
  df_caixin_pmi['Date'] = df_caixin_pmi['Month_Day'] + df_caixin_pmi['Year']
  df_caixin_pmi['Date'] = pd.to_datetime(df_caixin_pmi['Date'].str.strip(), format='%b %d %Y')
  df_caixin_pmi['Prev_Month'] = pd.to_datetime(df_caixin_pmi['Prev_Month'].str.strip(), format='%b')

  #Need to fix date if release date and month of data do not match
  for index, row in df_caixin_pmi.iterrows():
    if(row['Date'].month != row['Prev_Month'].month):
      year = dt.now().year
      if(row['Prev_Month'].month == 12): #If the previous month is december, we need to go back 1 year from this year
        year = year - 1    

      day = calendar.monthrange(year,row['Prev_Month'].month)[1]
      new_date = dt.strptime("%s-%s-%s" % (year,row['Prev_Month'].month,day), "%Y-%m-%d") 
      df_caixin_pmi.at[index,'Date'] = new_date

  #Drop unnecessary columns
  df_caixin_pmi = df_caixin_pmi.drop(columns='Release Date', axis=1)
  df_caixin_pmi = df_caixin_pmi.drop(columns='Month_Day', axis=1)
  df_caixin_pmi = df_caixin_pmi.drop(columns='Year_Temp', axis=1)
  df_caixin_pmi = df_caixin_pmi.drop(columns='Year', axis=1)
  df_caixin_pmi = df_caixin_pmi.drop(columns='Time', axis=1)
  df_caixin_pmi = df_caixin_pmi.drop(columns='Forecast', axis=1)
  df_caixin_pmi = df_caixin_pmi.drop(columns='Previous', axis=1)
  df_caixin_pmi = df_caixin_pmi.drop(columns='', axis=1)
  print(df_caixin_pmi)
  df_caixin_pmi = df_caixin_pmi.drop(columns='Prev_Month', axis=1)

  #Rename and reformat column
  df_caixin_pmi = df_caixin_pmi.rename(columns={"Actual": "HSBC China PMI"})
  df_caixin_pmi['HSBC China PMI'] = pd.to_numeric(df_caixin_pmi['HSBC China PMI'])

  # Reorder Columns
  cols = list(df_caixin_pmi)
  cols.insert(0, cols.pop(cols.index('Date')))
  cols.insert(1, cols.pop(cols.index('HSBC China PMI')))
  df_caixin_pmi = df_caixin_pmi[cols]

  return df_caixin_pmi

def scrape_table_china_industrial_production():
  #currentYear = dt.now().strftime('%Y')
  url_ip_yoy = 'https://tradingeconomics.com/china/industrial-production'

  #TODO: **********Change so that we extract the percentage growth from table rather than paragraph************

  # COPY CAIXIN:
  #      Date                  HSBC China PMI
  #    0 2022-03-31             NaN
  #    1 2022-02-28            50.4
  #    2 2022-01-29            49.1
  #    3 2021-12-31            50.9
  #    4 2021-11-30            49.9
  #    5 2021-10-31            50.6
  #    (Pdb) df_caixin_pmi.dtypes
  #    Date              datetime64[ns]
  #    HSBC China PMI           float64
   #   dtype: object


  #Get IP YoY Percentage Growth using Regex
  page = requests.get(url=url_ip_yoy)
  soup = BeautifulSoup(page.content, 'html.parser')
  
  table = soup.find('table') 
  df_ip_yoy = convert_html_table_to_df(table, False)

  #Rename Columns
  df_ip_yoy.rename(columns={ df_ip_yoy.columns[0]: "Release_Date",df_ip_yoy.columns[2]: "Month",df_ip_yoy.columns[3]: "YoY" }, inplace = True)

  #Remove unnecessary columns
  df_ip_yoy = df_ip_yoy.drop(['GMT', 'Previous', 'Consensus', 'TEForecast'], axis=1)

  #If the Month col contains multiple months, split them up into multiple rows
  df_ip_yoy = df_ip_yoy.assign(Month=df_ip_yoy.Month.str.split("-")).explode('Month')

  #Reset index
  df_ip_yoy = df_ip_yoy.reset_index(drop=True)

  import pdb; pdb.set_trace()
  #TODO: Create new Date field with correct Month and Year
  #TODO: Reformat actual value
  #   
  """
  #get the paragraph with the sentence about latest china percentage growth in month and year
  paragraph = soup.find("h2", attrs={'id': 'description'})

  # Use https://pythex.org/ to check that regex are selecting correctly
  pattern_select = re.compile(r'([\-]?[0-9]\.[0-9]\s(?=percent\s)[A-Za-z,&;\s\-0-9]*,)')
  matches = re.findall(pattern_select, paragraph.text)#pattern_select.finditer(paragraph.text)
  import pdb; pdb.set_trace()
  #Use regex to extract percentage, month and year from string
  pattern_percentage = re.compile(r'([\-]?[0-9]\.[0-9]\s(?=percent\s))')
  pattern_month_year = re.compile(r'(\b(?:Jan(?:uary)?|Feb(?:ruary)?|...|Dec(?:ember)?) (?:19[7-9]\d|2\d{3})(?=\D|$))')
  match_percentage = re.findall(pattern_percentage,matches[0])
  match_month_year = re.findall(pattern_month_year,matches[0])

  df_ip_yoy = pd.DataFrame()

  # Put match_percentage, match_month_year into df with correct data types
  # Return relevant dfs so that they can be used to write to excel
  df_ip_yoy.insert(0,"Date",[pd.to_datetime(match_month_year[0], format='%B %Y')],True)
  df_ip_yoy.insert(0,"YoY",[match_percentage[0].strip()],True)

  # Reorder Columns
  cols = list(df_ip_yoy)
  cols.insert(0, cols.pop(cols.index('Date')))
  cols.insert(1, cols.pop(cols.index('YoY')))
  df_ip_yoy = df_ip_yoy[cols]

  df_ip_yoy['YoY'] = pd.to_numeric(df_ip_yoy['YoY'])/100
  """

  return df_ip_yoy

#####################################
#   Get Capital Investment Data     #
#####################################
"""
sheet_name = 'Data World GDP'

# Use worldbank API to get capital investment data
#Get Capital Investment Data for the following countries
country_list = ['USA','CHN','CEB','JPN','DEU','GBR','FRA','IND','ITA','CAN','KOR','RUS','BRA','AUS','ESP','MEX','IDN','NLD']
#'EUN',
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

df_world_gdp = scrape_world_gdp_table("https://tradingeconomics.com/matrix")

#Drop unnecessary columns
col_drop = ['GDP YoY','GDP QoQ','Interest rate','Inflation rate','Jobless rate','Gov. Budget','Debt/GDP','Current Account','Currency','Population']
df_world_gdp = df_world_gdp.drop(col_drop, axis=1)

#Fix datatypes of df_world_gdp
df_world_gdp['GDP'] = pd.to_numeric(df_world_gdp['GDP'])
df_updated = pd.merge(df_updated,df_world_gdp,"right", on='Country')

#Drop unnecessary columns
df_updated_1 = df_updated.drop(columns='GDP_x', axis=1)
df_updated_2 = df_updated_1.rename(columns={'GDP_y': 'GDP'})

column_names = ["Country", "GDP", "Investment_Percentage", "Code"]

df_updated_3 = df_updated_2.reindex(columns=column_names)

write_dataframe_to_excel(excel_file_path, sheet_name, df_updated_3, False, -1)

##################################################
#   Get World IP Data from Trading Economics     #
##################################################

sheet_name = 'World Production data'

#Get World Production Data
df_world_production = scrape_table_world_production("https://tradingeconomics.com/country-list/industrial-production?continent=world")

df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

#Fix datatypes of df_world_gdp
df_world_production['Last'] = pd.to_numeric(df_world_production['Last'])
df_world_production['Previous'] = pd.to_numeric(df_world_production['Previous'])

df_updated = combine_df_on_index(df_original, df_world_production,'Country')

# get a list of columns
cols = list(df_updated)
# move the column to head of list
cols.insert(0, cols.pop(cols.index('Country')))
cols.insert(1, cols.pop(cols.index('Last')))
cols.insert(2, cols.pop(cols.index('Month')))
cols.insert(3, cols.pop(cols.index('Previous')))
cols.insert(4, cols.pop(cols.index('Unit')))

# reorder
df_updated = df_updated[cols]

write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, -1)

##################################################
#   Get China IP Data from Trading Economics     #
##################################################

sheet_name = 'China Production data'

#Get China Production Data
df_caixin_pmi = scrape_table_china_caixin_pmi()
"""
df_ip_yoy = scrape_table_china_industrial_production()

#temporary field called period_month so that we can combine df_caixin_pmi and df_ip_yoy together on Month only
df_caixin_pmi['period_month'] = pd.DatetimeIndex(df_caixin_pmi['Date']).month
df_ip_yoy['period_month'] = pd.DatetimeIndex(df_ip_yoy['Date']).month

#Combine on temp field period_month
df_china_pmi = combine_df_on_index(df_caixin_pmi, df_ip_yoy,'period_month')

#Combine finished, so we dont need period_month anymore
df_china_pmi = df_china_pmi.drop(columns='period_month', axis=1)

df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

df_updated = combine_df_on_index(df_original, df_china_pmi,'Date')

write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

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
df_world_industrial_production = df_world_industrial_production.drop(columns='MTH', axis=1)

# Check for difference between original and new lists
#print(util_check_diff_list(df_world_industrial_production.columns.tolist(), df_original.columns.tolist()))

df_updated_world_industrial_production = combine_df_on_index(df_original, df_world_industrial_production,'DATE')

# get a list of columns
cols = list(df_updated_world_industrial_production)
# move the column to head of list using index, pop and insert
cols.insert(0, cols.pop(cols.index('DATE')))

# reorder
df_updated_world_industrial_production = df_updated_world_industrial_production[cols]

# format date
df_updated_world_industrial_production['DATE'] = pd.to_datetime(df_updated_world_industrial_production['DATE'],format='%d/%m/%Y')

write_dataframe_to_excel(excel_file_path, sheet_name, df_updated_world_industrial_production, False, 0)

print("Done!")
