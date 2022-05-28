import requests
import sys
import json

import pandas as pd
from datetime import date
from common import get_oecd_data, convert_excelsheet_to_dataframe, write_dataframe_to_excel
from common import combine_df_on_index, get_yf_data

excel_file_path = '/Trading_Excel_Files/04_Filtering_Process/030_Filtering_Process_Quantitative_Analysis_Stock_One_Page.xlsm'

fmpcloud_account_key = '14afe305132a682a2742743df532707d'

def get_fmpcloud_data(url, filename):

    #TODO: check if current file has todays system date, and if it does load from current file. Otherwise, continue to call the api
    file_path = "%s/JSON/%s" % (sys.path[0],filename)
    data_list = []

    try:
        #TODO: Check if file date is today. If so, continue. Otherwise, throw exception so that we can use the API instead to load the data

        # opening the file in read mode
        my_file = open(file_path, "r")        
        data = my_file.read()
        
        # replacing end splitting the text 
        # when newline ('\n') is seen.
        liststr = data.split("\n")
        #print(data_into_list)
        my_file.close()
        data_list = eval(liststr[0])

    except:

        data_list.append(requests.get(url).json())

        # Write response to an XML File
        with open(file_path, 'w') as f:
            for item in data_list:
                f.write("%s\n" % item)

    return data_list

sheet_name = 'Database S&P500'
df_sp_500 = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

url = "https://fmpcloud.io/api/v3/quotes/index?apikey=%s" % (fmpcloud_account_key)

sp_index = '^GSPC'

data = get_fmpcloud_data(url,'030_SP500_details.json')

sp_price = ""

#TODO: Ensure that whether loaded from file or api, we are able to get the price.
for index in range(len(data)):
    for key in data[index]:
        if key['symbol'] == '^GSPC':
            sp_price = key['price']

print(sp_price)

import pdb; pdb.set_trace()


#All Indexes: https://fmpcloud.io/api/v3/quotes/index?apikey=14afe305132a682a2742743df532707d

#Winners and Losers:
# https://fmpcloud.io/api/v3/actives?apikey=14afe305132a682a2742743df532707d
# https://fmpcloud.io/api/v3/losers?apikey=14afe305132a682a2742743df532707d
# https://fmpcloud.io/api/v3/gainers?apikey=14afe305132a682a2742743df532707d
# https://fmpcloud.io/api/v3/sectors-performance?apikey=14afe305132a682a2742743df532707d

# Industries PE Ratio: https://fmpcloud.io/api/v4/industry_price_earning_ratio?date=2021-05-07&exchange=NYSE&apikey=14afe305132a682a2742743df532707d
# Sectors PE Ratio: https://fmpcloud.io/api/v4/sector_price_earning_ratio?date=2021-05-07&exchange=NYSE&apikey=14afe305132a682a2742743df532707d

# Commitment of Traders: https://fmpcloud.io/api/v4/commitment_of_traders_report/ES?apikey=14afe305132a682a2742743df532707d
# https://fmpcloud.io/api/v4/commitment_of_traders_report_analysis?from=2020-09-12&to=2021-01-01&apikey=14afe305132a682a2742743df532707d
# https://fmpcloud.io/api/v4/commitment_of_traders_report_analysis/M6?apikey=14afe305132a682a2742743df532707d

# Finwiz company quote: https://finviz.com/quote.ashx?t=CRM
#Company Real Time Quote: https://fmpcloud.io/api/v3/quote/AAPL?apikey=14afe305132a682a2742743df532707d
# Company Profile: https://fmpcloud.io/api/v3/profile/CRM?apikey=14afe305132a682a2742743df532707d
# Company Key Metrics: https://fmpcloud.io/api/v3/key-metrics-ttm/CRM?limit=40&apikey=14afe305132a682a2742743df532707d
# Financial Statements Growth: https://fmpcloud.io/api/v3/financial-growth/CRM?limit=20&apikey=14afe305132a682a2742743df532707d
# Company Earnings Call Transcripts: https://fmpcloud.io/api/v3/earning_call_transcript/CRM?quarter=3&year=2020&apikey=14afe305132a682a2742743df532707d
# Company Earnings Surprises: https://fmpcloud.io/api/v3/earnings-surpises/AAPL?apikey=14afe305132a682a2742743df532707d
# Company Peer List: https://fmpcloud.io/api/v4/stock_peers?symbol=CRM&apikey=14afe305132a682a2742743df532707d

"""
Company Name
Ticker
Description of company
Sector
Industry
5y historical sales growth
Sales Growth Current Year (F1)
Earnings Growth Current Year (F1)
Projected Earnings Growth Next Year (F2)
Dividend Yield %
Operating Margin 12 Month %
Net Martin %
Quick Ratio
Current Ratio
Debt/Equity Ratio
Debt/Total Capital
Price/Sales
Price/Book
Current ROE

Last
52 week high
52 week low
YTD change/%
Mkt Cap
EV
Days to Cover
Target Price
Trailing P/E
Forward P/E
PEG
Dividend 2019
Div. yield
Beta
Price to book
ROE
Exchange
Sector
Industry
Website

Current, Y-1, Y-2, Y+1(E), Y+2(E), Y+3(E)
-------------------------
Sales
yoy
EBITDA
EBITDA margin
Operating Profit (EBIT)
EBIT margin
Net income
Net Margin
P/E ratio
EPS
yoy
EV/EBITDA
EV/EBIT
EV/Revenues
Debt
EBITDA
Debt /EBITDA
Cash Flow per share
Book Value per share
----------------------

Volume
Avg Vol 10 days
Avg Vol 3Months

50 MAV
200 MAV

Buyback Year
Buyback Quarter

Sales Per Region

Competitors x4
---------------
Mkt Cap
EV
P/E
EV/EBITDA
EV/EBIT
EV/Revenues
PB
EBITDA margin
EBIT margin
Net margin
Dividend Yield
ROE

Historical Earnings Surprises
"""


def get_fmpcloud_data(url):

  dim_args = ['+'.join(d) for d in dimensions]
  dim_str = '.'.join(dim_args).replace('..','.')

  url = "https://fmpcloud.io/api/v3/stock-screener?sector={sector}&marketCapMoreThan={marketCap}&limit={limit}&apikey={demo_account_key}').json()"

  file_path = "%s/XML/%s" % (sys.path[0],params['filename'])

  resp = requests.get(url=url)

  try:
      resp.raise_for_status()
  except requests.exceptions.HTTPError as e:
      # Whoops it wasn't a 200

      if(resp.status_code == 400):
        #It didnt work with the original order of the params so lets try again
        url = "https://stats.oecd.org/restsdmx/sdmx.ashx/GetData/%s/%s.%s.%s.%s/all?startTime=%s&endTime=%s" % (dataset, dim_args[1],dim_args[0],dim_args[2],dim_args[3],params['startTime'],params['endTime'])

        #clean up any situation where the format of the url is broken
        url = url.replace('..','.')

        resp = get_page(url)

      else:
            # Whoops it wasn't a 200 
            raise Exception("Http Response (%s) Is Not 200: %s" % (url,str(resp.status_code)))

  resp_formatted = resp.text[resp.text.find('<'):len(resp.text)]
  # Write response to an XML File
  with open(file_path, 'w') as f:
    f.write(resp_formatted)

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




#if the api call was called today, get the output from the serialized pickle files. Otherwise, call the api to get the output
if(api_loaded_today):
    companies, balancesheet_list, incomestatemnt_list, cashflowstatemnt_list, marketcap_data_list, companydata_list, technological_companies = load_data_from_pickle()
else:
    companies, balancesheet_list, incomestatemnt_list, cashflowstatemnt_list, marketcap_data_list, companydata_list, technological_companies = load_data_from_api(sector, marketCap, limit, demo_account_key)





print("Done!")
