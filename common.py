import sys
from lib2to3.pgen2.pgen import DFAState
from pandas.io import excel
import requests
import sys
import os.path
import csv
import pandas as pd
import xml.etree.ElementTree as ET
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows
from itertools import islice
from datetime import date
from datetime import datetime as dt
from dateutil import parser, relativedelta
import time
from bs4 import BeautifulSoup
from requests.models import parse_header_links
import re
import yfinance as yf
import investpy as invest

############################
# Data Retrieval Functions #
############################

def get_stlouisfed_data(series_code):
  url = "https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=8067a107f45ff78491c1e3117245a0a3&file_type=json" % (series_code,)

  resp = requests.get(url=url)
  json = resp.json() 
  
  df = pd.DataFrame(columns=["DATE",series_code])
  # Convert the Date into Time Series Date
  # https://www.youtube.com/watch?v=UFuo7EHI8zc

  for i in range(len(json["observations"])):
    if(json["observations"][i]["value"] == '.'):
      obs = '0.00'
    else:
      obs = json["observations"][i]["value"]
    df = df.append({"DATE": json["observations"][i]["date"], series_code: obs}, ignore_index=True)

  df['DATE'] = pd.to_datetime(df['DATE'],format='%Y-%m-%d')
  df[series_code] = df[series_code].astype('float64') 
  #df = df.set_index('DATE')

  print("Retrieved Data for Series %s" % (series_code,))

  return df


# Get US GDP from St Louis FRED #
"""
def get_us_gdp_fred():
  df_GDPC1 = get_stlouisfed_data('GDPC1')

  df_GDPC1['GDPQoQ'] = (df_GDPC1['GDPC1'] - df_GDPC1['GDPC1'].shift()) / df_GDPC1['GDPC1'].shift()
  df_GDPC1['GDPYoY'] = (df_GDPC1['GDPC1'] - df_GDPC1['GDPC1'].shift(periods=4)) / df_GDPC1['GDPC1'].shift(periods=4)

  #Calculate QoQ Annualized growth rate =((1 + df_GDPC1['GDPQoQ'])^4)-1
  df_GDPC1['GDPQoQ_ANNUALIZED'] = ((1 + df_GDPC1['GDPQoQ']) ** 4) - 1

  return df_GDPC1


# Get GDP Data from St Louis FRED #
def get_gdp_fred(series_name):

  df_GDP = get_stlouisfed_data(series_name)

  df_GDP['GDPQoQ'] = (df_GDP[series_name] - df_GDP[series_name].shift()) / df_GDP[series_name].shift()
  df_GDP['GDPYoY'] = (df_GDP[series_name] - df_GDP[series_name].shift(periods=4)) / df_GDP[series_name].shift(periods=4)

  #Calculate QoQ Annualized growth rate =((1 + df_GDPC1['GDPQoQ'])^4)-1
  df_GDP['GDPQoQ_ANNUALIZED'] = ((1 + df_GDP['GDPQoQ']) ** 4) - 1

  return df_GDP
"""

def get_data_fred(series_name, col_name, period):

  df = get_stlouisfed_data(series_name)
  df = df.rename(columns={series_name: col_name})

  if(period=='Q'):
    df['%s_QoQ' % (col_name,)] = (df[col_name] - df[col_name].shift()) / df[col_name].shift()
    df['%s_YoY' % (col_name,)] = (df[col_name] - df[col_name].shift(periods=4)) / df[col_name].shift(periods=4)
    df['%s_QoQ_ANNUALIZED' % (col_name,)] = ((1 + df['%s_QoQ' % (col_name)]) ** 4) - 1

  elif(period=='M'):
    df['%s_MoM' % (col_name,)] = (df[col_name] - df[col_name].shift()) / df[col_name].shift()
    df['%s_QoQ' % (col_name,)] = (df[col_name] - df[col_name].shift(periods=4)) / df[col_name].shift(periods=4)
    df['%s_YoY' % (col_name,)] = (df[col_name] - df[col_name].shift(periods=12)) / df[col_name].shift(periods=12)
    df['%s_QoQ_ANNUALIZED' % (col_name,)] = ((1 + df['%s_QoQ' % (col_name)]) ** 12) - 1

  return df

def get_oecd_data(dataset, dimensions, params):

  dim_args = ['+'.join(d) for d in dimensions]
  dim_str = '.'.join(dim_args).replace('..','.')

  date_range = dimensions[3][0]
  match date_range:
    case 'Q':
      date_range = 'QTR'
    case 'M':
      date_range = 'MTH'

  url = "https://stats.oecd.org/restsdmx/sdmx.ashx/GetData/%s/%s/all?startTime=%s&endTime=%s" % (dataset, dim_str,params['startTime'],params['endTime'])
  #file_path = '/Users/markmukherjee/Documents/PythonProjects/PortfolioManagement/XML/%s' % params['filename'] 
  file_path = "%s/XML/%s" % (sys.path[0],params['filename'])

  try:
    #resp = requests.get(url=url,params=params)
    #resp = requests.get(url=url,verify=False)
    resp = requests.get(url=url)

    if(resp.status_code == 400):
      #It didnt work with the original order of the params so lets try again
      url = "https://stats.oecd.org/restsdmx/sdmx.ashx/GetData/%s/%s.%s.%s.%s/all?startTime=%s&endTime=%s" % (dataset, dim_args[1],dim_args[0],dim_args[2],dim_args[3],params['startTime'],params['endTime'])

      #clean up any situation where the format of the url is broken
      url = url.replace('..','.')

      #resp = requests.get(url=url,verify=False)
      resp = requests.get(url=url)

    resp_formatted = resp.text[resp.text.find('<'):len(resp.text)]
    # Write response to an XML File
    with open(file_path, 'w') as f:
      f.write(resp_formatted)

  except requests.exceptions.ConnectionError:
    print("Connection refused, Opening from File...")

  # Load in the XML file into ElementTree
  tree = ET.parse(file_path)

  #Load into a dataframe and return the data frame
  root = tree.getroot()

  ns = {'sdmx': 'http://www.SDMX.org/resources/SDMXML/schemas/v2_0/generic'}

  df = pd.DataFrame()

  #Add column headers
  df.insert(0,date_range,[],True)
  df.insert(1,"DATE",[],True)
  index = 2
  for series in root.findall('./sdmx:DataSet/sdmx:Series',ns):
    current_country = ""
    for value in series.findall('./sdmx:SeriesKey/sdmx:Value',ns): 
      if(value.get('concept')) == 'LOCATION':
        current_country = value.get('value')
        
        df.insert(index,value.get('value'),[],True)
    index+=1

  # This needs to account for both Quarterly and Monthly data.
  date_range_list = []
  date_list = []
  year_start, qtr_start =  params['startTime'].split('-Q')
  year_end, qtr_end =  params['endTime'].split('-Q')

  match date_range:
    case 'QTR':
      #From year_start to year_end, calculate all the quarters. Populate date_range_list and date_list
      date_list = pd.date_range('%s-01-01' % (year_start),'%s-01-01' % (int(year_end)+1), freq='QS').strftime("1/%-m/%Y").tolist()
      date_ranges = pd.PeriodIndex(pd.to_datetime(date_list, format='%d/%m/%Y'),freq='Q').strftime('%Y-Q%q')

      date_range_list = date_ranges.tolist()

      # Need to align QTR and DATE between df_original and df_QoQ.
      date_list.pop(0)
      date_range_list.pop()
    case 'MTH':
      #From year_start to year_end, calculate all the months. Populate date_range_list and date_list
      date_range_list = pd.date_range('%s-01-01' % (year_start),'%s-01-01' % (int(year_end)+1), freq='MS').strftime("%Y-%m").tolist()
      date_list = pd.date_range('%s-01-01' % (year_start),'%s-01-01' % (int(year_end)+1), freq='MS').strftime("1/%-m/%Y").tolist()

  df[date_range] = date_range_list
  df['DATE'] = date_list

  #Add observations for all countries
  for series in root.findall('./sdmx:DataSet/sdmx:Series',ns):
    for value in series.findall('./sdmx:SeriesKey/sdmx:Value',ns): 
      if(value.get('concept')) == 'LOCATION':
        current_country = value.get('value')

        observations = series.findall('sdmx:Obs',ns)

        for observation in observations:
          #Get qtr, date and observation. Add to column in chronological order for all countries

          obs_qtr = observation.findall('sdmx:Time',ns)[0].text
          obs_row = df.index[df[date_range] == obs_qtr].tolist()
          obs_value = observation.findall('sdmx:ObsValue',ns)[0].get('value')

          #match based on quarter and country, then add the observation value
          df.loc[obs_row, current_country] = round(float(obs_value),9)        

  #Set the date to the correct datatype, and ensure the format accounts for the correct positioning of day and month values
  df['DATE'] = pd.to_datetime(df['DATE'],format='%d/%m/%Y')

  print("Retrieved Data for Series %s" % (dataset,))

  return df

  #except Exception as e:
    
  #  exc_type, exc_obj, exc_tb = sys.exc_info()
  #  fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
  #  print(exc_type, fname, exc_tb.tb_lineno)

def get_yf_data(ticker, interval, start, end):
  data = yf.download(  # or pdr.get_data_yahoo(...
    # tickers list or string as well
    tickers = ticker,

    start=start, 
    end=end, 

    # use "period" instead of start/end
    # valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
    # (optional, default is '1mo')
    period = "ytd",

    # fetch data by interval (including intraday if period < 60 days)
    # valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
    # (optional, default is '1d')
    interval = interval,

    # group by ticker (to access via data['SPY'])
    # (optional, default is 'column')
    group_by = 'ticker',

    # adjust all OHLC automatically
    # (optional, default is False)
    auto_adjust = True,

    # download pre/post regular market hours data
    # (optional, default is False)
    prepost = True,

    # use threads for mass downloading? (True/False/Integer)
    # (optional, default is True)
    threads = True,

    # proxy URL scheme use use when downloading?
    # (optional, default is None)
    proxy = None
  )

  df_yf = data.reset_index()
  df_yf = df_yf.rename(columns={"Date": "DATE"})

  return df_yf

# Get S&P500 Monthly Close Prices from YF #
def get_sp500_monthly_prices():
  todays_date = date.today()
  date_str = "%s-%s-%s" % (todays_date.year, todays_date.month, "01")

  # Get S&P500 close month intervals using above date range
  df_SP500 = get_yf_data("^GSPC", "1mo", "1959-01-01", date_str)

  #Remove unnecessary columns from df_SP500 and rename columns
  df_SP500 = df_SP500.drop(['Open', 'High', 'Low', 'Volume'], axis=1)
  df_SP500 = df_SP500.rename(columns={"Close": "SP500"})

  return df_SP500

def get_invest_data(country_list, bond_year, from_date):

  todays_date = date.today()
  todays_date_full = "%s/%s/%s" % (todays_date.day, todays_date.month, todays_date.year)

  df = pd.DataFrame()
  df.insert(0,"DATE",[],True)

  for country in country_list:
    bond = "%s %sy" % (country, bond_year)

    print("Getting %s-Y Data For: %s" % (bond_year, country,))

    time.sleep(10)
    try:
      data = invest.get_bond_historical_data(bond=bond, from_date=from_date, to_date=todays_date_full)

      # Transformations to do: 
      # 1) get the relevant column
      # 2) convert series to df
      # 3) make the index a column
      # 4) Rename columns
      # 5) Append to master df

      data = data['Close']
      data = data.to_frame()
      data.reset_index(level=0, inplace=True)
      data = data.rename(columns={"Date": "DATE" ,"Close": country})
      df = append_two_df(df, data)

    except RuntimeError as e:
      print("NO %s-Y DATA FOR: %s" % (bond_year, country,))

  df = df.sort_values(by='DATE', ignore_index=True)

  return df

def scrape_world_gdp_table(url):
  #Scrape GDP Table from Trading Economics
  #url = "https://tradingeconomics.com/matrix"

  page = requests.get(url=url)
  soup = BeautifulSoup(page.content, 'html.parser')

  table = soup.find('table')
  table_rows = table.find_all('tr', attrs={'align':'center'})
  table_rows_header = table.find_all('tr')[0].find_all('th')
  df = pd.DataFrame()

  index = 0
  for header in table_rows_header:
    if(index == 0):
      df.insert(0,"Country",[],True)
    else:
      df.insert(index,header.text,[],True)

    index+=1

  #Insert New Row. Format the data to show percentage as float

  for tr in table_rows:
    temp_row = []
    first_col = True

    td = tr.find_all('td')
    for obs in td:
      if(first_col):
        text = ''.join(e for e in obs.text if e.isalnum())
        text = re.sub("([A-Z])", " \\1", text).strip()
      else:
        if(obs.text.find('%') < 0):
          text = obs.text
        else:
          text = obs.text.strip('%')
          text = float(text.strip('%'))/100

      temp_row.append(text)        
      first_col = False

    df.loc[len(df.index)] = temp_row

  return df

def get_ism_manufacturing_content():

  ism_date, ism_month = get_ism_date(1)
  url_ism = get_ism_manufacturing_url(ism_month)

  #*page = requests.get(url=url_ism,verify=False)
  page = requests.get(url=url_ism)

  if(page.status_code == 404):
      # Use previous month to get ISM data
      ism_date, ism_month = get_ism_date(2)
      url_ism = get_ism_manufacturing_url(ism_month)

      #*page = requests.get(url=url_ism,verify=False)
      page = requests.get(url=url_ism)

  return ism_date, ism_month, page

def get_ism_services_content():

  ism_date, ism_month = get_ism_date(1)
  url_ism = get_ism_services_url(ism_month)

  #*page = requests.get(url=url_ism,verify=False)
  page = requests.get(url=url_ism)

  if(page.status_code == 404):
      # Use previous month to get ISM data
      ism_date, ism_month = get_ism_date(2)
      url_ism = get_ism_services_url(ism_month)

      #*page = requests.get(url=url_ism,verify=False)
      page = requests.get(url=url_ism)

  return ism_date, ism_month, page


def get_ism_manufacturing_url(ism_month):
  url_ism = 'https://www.ismworld.org/supply-management-news-and-reports/reports/ism-report-on-business/pmi/%s' % (ism_month.lower(),)

  return url_ism

def get_ism_services_url(ism_month):
  url_ism = 'https://www.ismworld.org/supply-management-news-and-reports/reports/ism-report-on-business/services/%s' % (ism_month.lower(),)

  return url_ism

def get_ism_date(delta):
  #get date range
  todays_date = date.today()

  #use todays date to get ism month (first day of last month) and use the date in scraping functions
  ism_date = todays_date - relativedelta.relativedelta(months=delta)
  ism_date = "01-%s-%s" % (ism_date.month, ism_date.year) #make the ism date the first day of ism month
  ism_date = dt.strptime(ism_date, "%d-%m-%Y")
  ism_month = ism_date.strftime("%B")

  return ism_date, ism_month


def scrape_ism_manufacturing_headline_index(ism_date, ism_month):

  url_ism = get_ism_manufacturing_url(ism_month)

  #*page = requests.get(url=url_ism,verify=False)
  page = requests.get(url=url_ism)

  soup = BeautifulSoup(page.content, 'html.parser')

  #Get all html tables on the page
  tables = soup.find_all('table')    
  table_at_a_glance = tables[0]
  
  #Convert the tables into dataframes so that we can read the data
  df_at_a_glance = convert_html_table_to_df(table_at_a_glance, True)

  #Drop Unnecessary Columns
  column_numbers = [x for x in range(df_at_a_glance.shape[1])]  # list of columns' integer indices
  column_numbers .remove(2) #removing column integer index 0
  column_numbers .remove(3)
  column_numbers .remove(4)
  column_numbers .remove(5)
  column_numbers .remove(6)
  df_at_a_glance = df_at_a_glance.iloc[:, column_numbers] #return all columns except the 0th column

  #Flip df around
  df_at_a_glance = df_at_a_glance.T

  #Rename Columns
  df_at_a_glance = df_at_a_glance.rename(columns={0: "ISM", 1:"NEW_ORDERS",2:"PRODUCTION",3:"EMPLOYMENT",4:"DELIVERIES",
                                                  5:"INVENTORIES",6:"CUSTOMERS_INVENTORIES",7:"PRICES",8:"BACKLOG_OF_ORDERS",9:"EXPORTS",10:"IMPORTS"})

  #Drop the first row because it contains the old column names
  df_at_a_glance = df_at_a_glance.iloc[1: , :]
  df_at_a_glance = df_at_a_glance.reset_index()
  df_at_a_glance = df_at_a_glance.drop(columns='index', axis=1)

  #Fix datatypes of df_at_a_glance
  for column in df_at_a_glance:
      df_at_a_glance[column] = pd.to_numeric(df_at_a_glance[column])

  #Add DATE column to df
  df_at_a_glance["DATE"] = [ism_date]

  # Reorder Columns
  # get a list of columns
  cols = list(df_at_a_glance)
  cols.insert(0, cols.pop(cols.index('DATE')))
  cols.insert(1, cols.pop(cols.index('NEW_ORDERS')))
  cols.insert(2, cols.pop(cols.index('IMPORTS')))
  cols.insert(3, cols.pop(cols.index('BACKLOG_OF_ORDERS')))
  cols.insert(4, cols.pop(cols.index('PRICES')))
  cols.insert(5, cols.pop(cols.index('PRODUCTION')))
  cols.insert(6, cols.pop(cols.index('CUSTOMERS_INVENTORIES')))
  cols.insert(7, cols.pop(cols.index('INVENTORIES')))
  cols.insert(8, cols.pop(cols.index('DELIVERIES')))
  cols.insert(9, cols.pop(cols.index('EMPLOYMENT')))
  cols.insert(10, cols.pop(cols.index('EXPORTS')))
  cols.insert(11, cols.pop(cols.index('ISM')))

  # reorder
  df_at_a_glance = df_at_a_glance[cols]

  return df_at_a_glance

def scrape_ism_services_headline_index(ism_date, ism_month):

    url_ism = get_ism_services_url(ism_month)

    #**page = requests.get(url=url_ism,verify=False)
    page = requests.get(url=url_ism)

    soup = BeautifulSoup(page.content, 'html.parser')

    #Get all html tables on the page
    tables = soup.find_all('table')    
    table_at_a_glance = tables[0]
    
    #Convert the tables into dataframes so that we can read the data
    #df_at_a_glance = convert_html_table_to_df(table_at_a_glance, True)

    table_rows = table_at_a_glance.find_all('tbody')[0].find_all('tr')
    table_rows_header = table_at_a_glance.find_all('tr')[1].find_all('th')
    df_at_a_glance = pd.DataFrame()

    index = 0

    for header in table_rows_header:
        df_at_a_glance.insert(index,str(header.text).strip(),[],True)
        index+=1

    #Insert New Row. Format the data to show percentage as float
    for tr in table_rows:
        temp_row = []

        tr_th = tr.find('th')
        text = str(tr_th.text).strip()
        temp_row.append(text)        

        td = tr.find_all('td')
        for obs in td:
            text = str(obs.text).strip()
            temp_row.append(text)        
        
        if(len(temp_row) == len(df_at_a_glance.columns)):
            df_at_a_glance.loc[len(df_at_a_glance.index)] = temp_row
    
    #Drop Unnecessary Columns
    column_numbers = [x for x in range(df_at_a_glance.shape[1])]  # list of columns' integer indices
    column_numbers .remove(7)
    column_numbers .remove(8)
    column_numbers .remove(9)

    df_at_a_glance = df_at_a_glance.iloc[:, column_numbers] #return all columns except the 0th column

    #Flip df around
    df_at_a_glance = df_at_a_glance.T

    # Rename Columns as per requirements of excel file 017
    df_at_a_glance = df_at_a_glance.rename(columns={0: "ISM_SERVICES", 1:"BUSINESS_ACTIVITY",2:"NEW_ORDERS",3:"EMPLOYMENT",4:"DELIVERIES",
                                                    5:"INVENTORIES",6:"PRICES",7:"BACKLOG_OF_ORDERS",8:"EXPORTS",9:"IMPORTS",10:"INVENTORY_SENTIMENT",11:"CUSTOMER_INVENTORIES"})

    #Drop the first row because it contains the old column names
    df_at_a_glance = df_at_a_glance.iloc[1: , :]
    df_at_a_glance = df_at_a_glance.head(1)
    df_at_a_glance = df_at_a_glance.reset_index()
    df_at_a_glance = df_at_a_glance.drop(columns='index', axis=1)
    df_at_a_glance = df_at_a_glance.drop(columns='CUSTOMER_INVENTORIES', axis=1)

    #Fix datatypes of df_at_a_glance
    for column in df_at_a_glance:
        df_at_a_glance[column] = pd.to_numeric(df_at_a_glance[column])

    #Add DATE column to df
    df_at_a_glance["DATE"] = [ism_date]

    # Put DATE as the first column
    # get a list of columns
    cols = list(df_at_a_glance)
    cols.insert(0, cols.pop(cols.index('DATE')))

    # reorder
    df_at_a_glance = df_at_a_glance[cols]

    return df_at_a_glance


####################
# Output Functions #
####################

def convert_excelsheet_to_dataframe(excel_file_path,sheet_name,date_exists=False, index_col=None, date_format='%d/%m/%Y'):

  filepath = os.path.realpath(__file__)
  excel_file_path = filepath[:filepath.rfind('/')] + excel_file_path

  if(index_col):
    df = pd.read_excel(excel_file_path, sheet_name=sheet_name, index_col=index_col, engine='openpyxl')
  else:
    df = pd.read_excel(excel_file_path, sheet_name=sheet_name, engine='openpyxl')

  if(date_exists):
    df['DATE'] = pd.to_datetime(df['DATE'],format=date_format)

  return df

def write_to_directory(df,filename):
    #Write to a csv file in the correct directory
    userhome = os.path.expanduser('~')
    file_name = os.path.join(userhome, 'Desktop', 'Trading_Excel_Files', 'Database',filename)
    df.to_csv(file_name, index=False)

def write_dataframe_to_excel(excel_file_path,sheet_name, df, include_index, date_position=None):

  filepath = os.path.realpath(__file__)
  excel_file_path = filepath[:filepath.rfind('/')] + excel_file_path

  book = openpyxl.load_workbook(excel_file_path, read_only=False, keep_vba=True)
  sheet = book[sheet_name]

  book.active = sheet

  # Delete all rows after the header so that we can replace them with our df  
  sheet.delete_rows(1,sheet.max_row)
  
  if(include_index):
    df.reset_index(level=0, inplace=True)
    # Need to remove additional unnecessary rows from beginning of df_QoQ dataframe
    df = df.iloc[1: , :].reset_index(drop=True)    

  #Write values from the df to the sheet
  for r in dataframe_to_rows(df,index=False, header=True):
    sheet.append(r)

  if(date_position >= 0):
    for row in sheet[2:sheet.max_row]: # skip the header
      cell = row[date_position]   # column date_position is a Date Field.
      cell.number_format = 'dd-mm-YYYY'
    
  book.save(excel_file_path)
  book.close()

####################
# Helper Functions #
####################

def combine_df(df_original, df_new):

  return df_original.combine(df_new, take_larger, overwrite=False)  

def append_two_df(df1, df2):
  merged_data = pd.merge(df1, df2, how='outer', on='DATE')
  return merged_data

def take_larger(s1, s2):
  return s2

def combine_df_on_index(df1, df2, index_col):
  df1 = df1.set_index(index_col)
  df2 = df2.set_index(index_col)

  return df1.combine_first(df2).reset_index()

def convert_html_table_to_df(table, contains_th):
  
  table_rows = table.find_all('tr')
  table_rows_header = table.find_all('tr')[0].find_all('th')
  df = pd.DataFrame()

  index = 0

  for header in table_rows_header:
    df.insert(index,str(header.text).strip(),[],True)
    index+=1

  #Insert New Row. Format the data to show percentage as float
  for tr in table_rows:
    temp_row = []

    if(contains_th):
      tr_th = tr.find('th')
      text = str(tr_th.text).strip()
      temp_row.append(text)        

    td = tr.find_all('td')
    for obs in td:
      
      exclude = False

      if(obs.find_all('div')):
        if 'hidden' in obs.find_all('div')[0].attrs['class']:
          exclude = True

      if not exclude:
        text = str(obs.text).strip()
        temp_row.append(text)        

    if(len(temp_row) == len(df.columns)):
      df.loc[len(df.index)] = temp_row
  
  return df

#Util function for transforming ISM data on sheet 016
def _transform_data(excel_file_path, sheet_name_original, sheet_name_new):

  df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name_original, False)

  df_original_transposed = df_original.transpose()

  new_header = df_original_transposed.iloc[0] #grab the first row for the header
  df1 = df_original_transposed[1:] #take the data less the header row
  df1.columns = new_header #set the header row as the df header
  df1 = df1.reset_index()
  df1 = df1.rename(columns={"index": "DATE"})

  for column in df1:
    if(column != 'DATE'):
      df1[column] = pd.to_numeric(df1[column])

  # Write the updated df back to the excel sheet
  write_dataframe_to_excel(excel_file_path, sheet_name_new, df1, False, 0)

def _util_check_diff_list(li1, li2):
  # Python code to get difference of two lists
  return list(set(li1) - set(li2))