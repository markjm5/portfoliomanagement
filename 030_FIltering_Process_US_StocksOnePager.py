import pandas as pd
from pandas.tseries.offsets import BDay
from bs4 import BeautifulSoup
from datetime import date
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
from common import get_oecd_data, convert_excelsheet_to_dataframe, write_dataframe_to_excel
from common import get_api_json_data,get_page, get_finwiz_stock_data, get_api_json_data_no_file, get_page_selenium, combine_df_on_index, write_value_to_cell_excel

#################
ticker = "AAPL" # COMPANY TICKER - CHANGE HERE
#################

fmpcloud_account_key = '14afe305132a682a2742743df532707d'

temp_excel_file_path = '/Trading_Excel_Files/04_Filtering_Process/030_Filtering_Process_Quantitative_Analysis_US_Stocks.xlsm'
temp_sheet_name = 'Database US Companies'

#Get company data from various sources
df_us_companies = convert_excelsheet_to_dataframe(temp_excel_file_path, temp_sheet_name, False)
df_zacks_stock_data = df_us_companies[df_us_companies['TICKER'] == ticker]
df_finwiz_stock_data = get_finwiz_stock_data(ticker)

excel_file_path = '/Trading_Excel_Files/04_Filtering_Process/030_Filtering_Process_Quantitative_Analysis_US_StocksOnePager.xlsm'

import pdb; pdb.set_trace()

todays_date = date.today()
one_year_ago = dt(todays_date.year - 1, 12, 31)
two_year_ago = dt(todays_date.year - 2, 12, 31)
three_year_ago = dt(todays_date.year - 3, 12, 31)

list_dates = []
list_dates.append(one_year_ago)
list_dates.append(two_year_ago)
list_dates.append(three_year_ago)

#Get Company Specific Data
df_company_details = df_us_companies.loc[df_us_companies['TICKER'] == ticker].reset_index(drop=True)

#urls:
url_finviz = "https://finviz.com/quote.ashx?t=%s" % (ticker)
url_company_profile = "https://fmpcloud.io/api/v3/profile/%s?apikey=%s" % (ticker,fmpcloud_account_key)
url_company_key_metrics = "https://fmpcloud.io/api/v3/key-metrics-ttm/%s?limit=40&apikey=%s"  % (ticker,fmpcloud_account_key)
url_company_peers = "https://fmpcloud.io/api/v4/stock_peers?symbol=%s&apikey=%s"  % (ticker,fmpcloud_account_key)
url_company_earnings_surprises = "https://fmpcloud.io/api/v3/earnings-surpises/%s?apikey=%s"  % (ticker,fmpcloud_account_key)
url_company_sec_filings = "https://fmpcloud.io/api/v3/financial-statements/%s?datatype=zip&apikey=%s" % (ticker,fmpcloud_account_key)

page = get_page(url_finviz)

soup = BeautifulSoup(page.content, 'html.parser')

table = soup.find_all('table')
table_rows = table[8].find_all('tr', recursive=False)

emptyDict = {}

#Get rows of data.
for tr in table_rows:
    tds = tr.find_all('td')
    boolKey = True
    keyValueSet = False
    for td in tds:
        if boolKey:
            key = td.text.strip()
            boolKey = False                
        else:
            value = td.text.strip()
            boolKey = True
            keyValueSet = True                

        if keyValueSet:
            emptyDict[key] = value
            keyValueSet = False

df_company_details.loc[df_company_details['TICKER'] == ticker, 'PE'] = emptyDict['P/E']
df_company_details.loc[df_company_details['TICKER'] == ticker, 'EPS_TTM'] = emptyDict['EPS (ttm)']
df_company_details.loc[df_company_details['TICKER'] == ticker, 'PE_FORWARD'] = emptyDict['Forward P/E']
df_company_details.loc[df_company_details['TICKER'] == ticker, 'EPS_Y1'] = emptyDict['EPS next Y']
df_company_details.loc[df_company_details['TICKER'] == ticker, 'PEG'] = emptyDict['PEG']
df_company_details.loc[df_company_details['TICKER'] == ticker, 'EPS_Y0'] = emptyDict['EPS this Y']
df_company_details.loc[df_company_details['TICKER'] == ticker, 'PRICE_BOOK'] = emptyDict['P/B']
df_company_details.loc[df_company_details['TICKER'] == ticker, 'PRICE_BOOK'] = emptyDict['P/B']
df_company_details.loc[df_company_details['TICKER'] == ticker, 'PRICE_SALES'] = emptyDict['P/S']
df_company_details.loc[df_company_details['TICKER'] == ticker, 'TARGET_PRICE'] = emptyDict['Target Price']
df_company_details.loc[df_company_details['TICKER'] == ticker, 'ROE'] = emptyDict['ROE']
df_company_details.loc[df_company_details['TICKER'] == ticker, '52W_RANGE'] = emptyDict['52W Range']
df_company_details.loc[df_company_details['TICKER'] == ticker, 'QUICK_RATIO'] = emptyDict['Quick Ratio']
df_company_details.loc[df_company_details['TICKER'] == ticker, 'GROSS_MARGIN'] = emptyDict['Gross Margin']
df_company_details.loc[df_company_details['TICKER'] == ticker, 'CURRENT_RATIO'] = emptyDict['Current Ratio']

import pdb; pdb.set_trace()
#https://finance.yahoo.com/
#https://www.reuters.com
#https://www.marketwatch.com/
#https://www.marketscreener.com/
#https://stockrow.com/



import pdb; pdb.set_trace()

"""
for index, row in df_us_companies.iterrows():
    print("Downloading Data For: %s - %s" % (row['TICKER'], row['COMPANY_NAME']))

    url_company_profile = "https://fmpcloud.io/api/v3/profile/%s?apikey=%s" % (row['TICKER'],nasdaq_data_api_key)
    data_company_profile = get_api_json_data_no_file(url_company_profile)

    df_us_companies.loc[df_us_companies['TICKER'] == data_company_profile[0][0]['symbol'], 'DESCRIPTION'] = data_company_profile[0][0]['description']

    url_company_sec_filings = "https://fmpcloud.io/api/v3/financial-statements/%s?datatype=zip&apikey=%s" % (row['TICKER'],nasdaq_data_api_key)

    
    for index in data_company_profile:   
        df_us_companies = df_us_companies.append({"DATE": dt.strptime(index['date'],"%Y-%m-%d"), "TICKER": row['sector'],"PE": pd.to_numeric(index['pe'])}, ignore_index=True)


    url_company_key_metrics = "https://fmpcloud.io/api/v3/key-metrics-ttm/CRM?limit=40&apikey=14afe305132a682a2742743df532707d"
    url_company_peers = "https://fmpcloud.io/api/v4/stock_peers?symbol=CRM&apikey=14afe305132a682a2742743df532707d"
    url_company_earnings_surprises = "https://fmpcloud.io/api/v3/earnings-surpises/AAPL?apikey=14afe305132a682a2742743df532707d"

    url_finviz = "https://finviz.com/quote.ashx?t=%s" % (row['TICKER'])
    try:
        page = get_page(url_finviz)

        soup = BeautifulSoup(page.content, 'html.parser')

        table = soup.find_all('table')
        table_rows = table[8].find_all('tr', recursive=False)

        emptyDict = {}

        #Get rows of data.
        for tr in table_rows:
            tds = tr.find_all('td')
            boolKey = True
            keyValueSet = False
            for td in tds:
                if boolKey:
                    key = td.text.strip()
                    boolKey = False                
                else:
                    value = td.text.strip()
                    boolKey = True
                    keyValueSet = True                

                if keyValueSet:
                    emptyDict[key] = value
                    keyValueSet = False

        df_us_companies.loc[df_us_companies['TICKER'] == row['TICKER'], 'PRICE_BOOK'] = emptyDict['P/B']
        df_us_companies.loc[df_us_companies['TICKER'] == row['TICKER'], 'PRICE_SALES'] = emptyDict['P/S']
        df_us_companies.loc[df_us_companies['TICKER'] == row['TICKER'], '52W_RANGE'] = emptyDict['52W Range']
    except:
        print("Exception: %s - %s" % (row['TICKER'],row['COMPANY_NAME']))
        pass
    """

import pdb; pdb.set_trace()

df_company_list = pd.DataFrame()

url = "https://fmpcloud.io/api/v3/stock-screener?&marketCapMoreThan=3000000000&exchange=NYSE,NASDAQ&isActivelyTrading=true&isEtf=false&apikey=%s" % (fmpcloud_account_key)
data_turnover_over_3m = get_api_json_data(url,'030_stocks_turnover_over_3m.json')

for index in data_turnover_over_3m:   
    if(index['exchangeShortName'] in ['NASDAQ', 'NYSE'] and index['sector'] != ''):
        df_company_list = df_company_list.append({"TICKER": index['symbol'], 
            "COMPANY": index['companyName'],"MARKET_CAP": pd.to_numeric(index['marketCap']),
            "SECTOR": index['sector'],"INDUSTRY": index['industry'],"BETA": pd.to_numeric(index['beta']),
            "LAST": pd.to_numeric(index['price']),"LAST_DIVIDEND": pd.to_numeric(index['lastAnnualDividend']),"EXCHANGE": index['exchange'],"VOLUME": pd.to_numeric(index['volume'])}, ignore_index=True)
        try:
            url_finviz = "https://finviz.com/quote.ashx?t=%s" % (index['symbol'])
            page = get_page(url_finviz)
        except:
            try:
                url_finviz = "https://finviz.com/quote.ashx?t=%s" % (index['symbol'].split('-')[0])
                page = get_page(url_finviz)

                soup = BeautifulSoup(page.content, 'html.parser')

                table = soup.find_all('table')
                table_rows = table[8].find_all('tr', recursive=False)

                temp_row = []
                emptyDict = {}

                #Get rows of data.
                for tr in table_rows:
                    tds = tr.find_all('td')
                    boolKey = True
                    keyValueSet = False
                    for td in tds:
                        if boolKey:
                            key = td.text.strip()
                            boolKey = False                
                        else:
                            value = td.text.strip()
                            boolKey = True
                            keyValueSet = True                

                        if keyValueSet:
                            emptyDict[key] = value
                            keyValueSet = False

                df_company_list.loc[df_company_list['TICKER'] == index['symbol'], 'PE'] = emptyDict['P/E']
                df_company_list.loc[df_company_list['TICKER'] == index['symbol'], 'EPS_TTM'] = emptyDict['EPS (ttm)']
                df_company_list.loc[df_company_list['TICKER'] == index['symbol'], 'PE_FORWARD'] = emptyDict['Forward P/E']
                df_company_list.loc[df_company_list['TICKER'] == index['symbol'], 'EPS_Y1'] = emptyDict['EPS next Y']
                df_company_list.loc[df_company_list['TICKER'] == index['symbol'], 'PEG'] = emptyDict['PEG']
                df_company_list.loc[df_company_list['TICKER'] == index['symbol'], 'EPS_Y0'] = emptyDict['EPS this Y']
                df_company_list.loc[df_company_list['TICKER'] == index['symbol'], 'PRICE_BOOK'] = emptyDict['P/B']
                df_company_list.loc[df_company_list['TICKER'] == index['symbol'], 'PRICE_BOOK'] = emptyDict['P/B']
                df_company_list.loc[df_company_list['TICKER'] == index['symbol'], 'PRICE_SALES'] = emptyDict['P/S']
                df_company_list.loc[df_company_list['TICKER'] == index['symbol'], 'TARGET_PRICE'] = emptyDict['Target Price']
                df_company_list.loc[df_company_list['TICKER'] == index['symbol'], 'ROE'] = emptyDict['ROE']
                df_company_list.loc[df_company_list['TICKER'] == index['symbol'], '52W_RANGE'] = emptyDict['52W Range']
                df_company_list.loc[df_company_list['TICKER'] == index['symbol'], 'QUICK_RATIO'] = emptyDict['Quick Ratio']
                df_company_list.loc[df_company_list['TICKER'] == index['symbol'], 'GROSS_MARGIN'] = emptyDict['Gross Margin']
                df_company_list.loc[df_company_list['TICKER'] == index['symbol'], 'CURRENT_RATIO'] = emptyDict['Current Ratio']
            except:
                print("Exception: %s - %s" % (index['symbol'],index['companyName']))
                pass
    """
    url_nasdaq = "https://www.nasdaq.com/market-activity/stocks/%s/price-earnings-peg-ratios" % (index['symbol'])
    page1 = get_page_selenium(url_nasdaq)
    soup1 = BeautifulSoup(page1, 'html.parser')

    table1 = soup1.find_all('table')
    table_rows1 = table1[0].find_all('tr', recursive=True)
    https://fmpcloud.io/api/v3/ratios/AAPL?limit=40&apikey=14afe305132a682a2742743df532707d
    url = "https://finviz.com/quote.ashx?t=%s"
    """

import pdb; pdb.set_trace()

"""
url = "https://data.nasdaq.com/api/v3/datatables/ZACKS/EE.json?api_key=%s" % (nasdaq_data_api_key)
data_earnings_estimates = get_api_json_data(url,'030_stock_earnings_estimates.json')

url = "https://data.nasdaq.com/api/v3/datatables/ZACKS/SE.json?api_key=%s" % (nasdaq_data_api_key)
data_sales_estimates = get_api_json_data(url,'030_stock_sales_estimates.json')

url = "https://data.nasdaq.com/api/v3/datatables/ZACKS/FC.json?api_key=%s" % (nasdaq_data_api_key)
data_fundamentals = get_api_json_data(url,'030_stock_fundamentals.json')

url = "https://data.nasdaq.com/api/v3/datatables/ZACKS/ES.json?api_key=%s" % (nasdaq_data_api_key)
data_earnings_surprises = get_api_json_data(url,'030_stock_earnings_surprises.json')
"""

import pdb; pdb.set_trace()
#Companies over 3 billion turnover: https://fmpcloud.io/api/v3/stock-screener?&marketCapMoreThan=3000000000&apikey=14afe305132a682a2742743df532707d
#Finwiz company quote: https://finviz.com/quote.ashx?t=CRM
#Income Statement: https://fmpcloud.io/api/v3/income-statement/AAPL?limit=120&apikey=14afe305132a682a2742743df532707d
#Balance Sheet: https://fmpcloud.io/api/v3/balance-sheet-statement/AAPL?limit=120&apikey=14afe305132a682a2742743df532707d
#Cash Flow Statement: https://fmpcloud.io/api/v3/cash-flow-statement/AAPL?limit=120&apikey=14afe305132a682a2742743df532707d
#Ratios: https://fmpcloud.io/api/v3/ratios/AAPL?limit=40&apikey=14afe305132a682a2742743df532707d

# Forward Estimates Ratios: https://www.nasdaq.com/market-activity/stocks/aapl/price-earnings-peg-ratios

#TODO: Convert the above json files into a dataframe

#Winners and Losers:
# https://fmpcloud.io/api/v3/actives?apikey=14afe305132a682a2742743df532707d
# https://fmpcloud.io/api/v3/losers?apikey=14afe305132a682a2742743df532707d
# https://fmpcloud.io/api/v3/gainers?apikey=14afe305132a682a2742743df532707d
# https://fmpcloud.io/api/v3/sectors-performance?apikey=14afe305132a682a2742743df532707d

# Finwiz company quote: https://finviz.com/quote.ashx?t=CRM
# Company Real Time Quote: https://fmpcloud.io/api/v3/quote/AAPL?apikey=14afe305132a682a2742743df532707d
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
print("Done!")
