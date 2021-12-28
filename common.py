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
import datetime
from bs4 import BeautifulSoup
from requests.models import parse_header_links
import re
import yfinance as yf

def get_stlouisfed_data(series_code):
  url = "https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=8067a107f45ff78491c1e3117245a0a3&file_type=json" % (series_code,)

  resp = requests.get(url=url)
  json = resp.json() 
  
  df = pd.DataFrame(columns=["DATE",series_code])
  #TODO: Convert the Date into Time Series Date
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

def get_oecd_data(dataset, dimensions, params):

  dim_args = ['+'.join(d) for d in dimensions]
  dim_str = '.'.join(dim_args)

  date_range = dimensions[3][0]
  match date_range:
    case 'Q':
      date_range = 'QTR'
    case 'M':
      date_range = 'MTH'

  url = "https://stats.oecd.org/restsdmx/sdmx.ashx/GetData/%s/%s/all?startTime=%s&endTime=%s" % (dataset, dim_str,params['startTime'],params['endTime'])
  file_path = 'XML/%s' % params['filename'] 
  try:
    #resp = requests.get(url=url,params=params)
    resp = requests.get(url=url,verify=False)

    if(resp.status_code == 400):
      url = "https://stats.oecd.org/restsdmx/sdmx.ashx/GetData/%s/%s.%s.%s.%s/all?startTime=%s&endTime=%s" % (dataset, dim_args[1],dim_args[0],dim_args[2],dim_args[3],params['startTime'],params['endTime'])

      resp = requests.get(url=url,verify=False)

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

  #Add Dates  TODO: This needs to account for both Quarterly and Monthly data.
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

  return df

  #except Exception as e:
    
  #  exc_type, exc_obj, exc_tb = sys.exc_info()
  #  fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
  #  print(exc_type, fname, exc_tb.tb_lineno)


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

def write_to_directory(df,filename):
    #Write to a csv file in the correct directory
    userhome = os.path.expanduser('~')
    file_name = os.path.join(userhome, 'Desktop', 'Trading_Excel_Files', 'Database',filename)
    df.to_csv(file_name, index=False)

def convert_excelsheet_to_dataframe(excel_file_path,sheet_name,date_exists=False, index_col=None):

  filepath = os.path.realpath(__file__)
  excel_file_path = filepath[:filepath.rfind('/')] + excel_file_path

  if(index_col):
    df = pd.read_excel(excel_file_path, sheet_name=sheet_name, index_col=index_col, engine='openpyxl')
  else:
    df = pd.read_excel(excel_file_path, sheet_name=sheet_name, engine='openpyxl')

  if(date_exists):
    df['DATE'] = pd.to_datetime(df['DATE'],format='%d/%m/%Y')

  return df

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

def combine_df(df_original, df_new):

  return df_original.combine(df_new, take_larger, overwrite=False)  
  
def util_check_diff_list(li1, li2):
  # Python code t get difference of two lists
  return list(set(li1) - set(li2))

def take_larger(s1, s2):
  return s2


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

def merge_two_df(df1, df2):

  #TODO: If df1 has a more recent date than df2, merge left. Otherwise, merge right
  df = pd.merge(df1,df2,"right")
  df = pd.merge(df1,df2,"left")
