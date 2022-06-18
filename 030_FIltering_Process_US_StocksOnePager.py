import pandas as pd
import json
#import calendar
import requests
from pandas.tseries.offsets import BDay
from bs4 import BeautifulSoup
from datetime import date
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
from common import convert_excelsheet_to_dataframe, get_stockrow_stock_data, write_dataframe_to_excel
from common import get_api_json_data,get_page, get_finwiz_stock_data, get_stockrow_stock_data, get_api_json_data_no_file
from common import combine_df_on_index, write_value_to_cell_excel, check_sheet_exists, create_sheet
from common import download_file, unzip_file, get_yf_key_stats
#Sources:
#https://finance.yahoo.com/
#https://www.reuters.com
#https://www.marketwatch.com/
#https://www.marketscreener.com/
#https://stockrow.com/

# Company Profile: https://finance.yahoo.com/quote/CRM/profile?p=CRM
# Company Profile: https://www.marketwatch.com/investing/stock/crm/company-profile
# Competitors: https://www.marketwatch.com/investing/stock/crm

# https://finance.yahoo.com/quote/CRM/key-statistics?p=CRM

# Available modules: - 'assetProfile', - 'summaryProfile', - 'summaryDetail', 
# - 'esgScores', - 'price', - 'incomeStatementHistory', 
# - 'incomeStatementHistoryQuarterly', - 'balanceSheetHistory', 
# - 'balanceSheetHistoryQuarterly', - 'cashflowStatementHistory', 
# - 'cashflowStatementHistoryQuarterly', - 'defaultKeyStatistics', 
# - 'financialData', - 'calendarEvents', - 'secFilings', - 'recommendationTrend', 
# - 'upgradeDowngradeHistory', - 'institutionOwnership', - 'fundOwnership', 
# - 'majorDirectHolders', - 'majorHoldersBreakdown', - 'insiderTransactions', 
# - 'insiderHolders', - 'netSharePurchaseActivity', - 'earnings', 
# - 'earningsHistory', - 'earningsTrend', - 'industryTrend', - 'indexTrend', 
# - 'sectorTrend'

# https://query2.finance.yahoo.com/v10/finance/quoteSummary/AAPL?modules=assetProfile,financialData,defaultKeyStatistics,calendarEvents

debug = True

#################
ticker = "AAPL" # COMPANY TICKER - CHANGE HERE
#################

fmpcloud_account_key = '14afe305132a682a2742743df532707d'

#Dates
todays_date = date.today()
one_year_ago = dt(todays_date.year - 1, 12, 31)
two_year_ago = dt(todays_date.year - 2, 12, 31)
three_year_ago = dt(todays_date.year - 3, 12, 31)
list_dates = []
list_dates.append(one_year_ago)
list_dates.append(two_year_ago)
list_dates.append(three_year_ago)

temp_excel_file_path = '/Trading_Excel_Files/04_Filtering_Process/030_Filtering_Process_Quantitative_Analysis_US_Stocks.xlsm'
temp_sheet_name = 'Database US Companies'

#Get company data from various sources
df_us_companies = convert_excelsheet_to_dataframe(temp_excel_file_path, temp_sheet_name, False)
df_zacks_stock_data = df_company_details = df_us_companies.loc[df_us_companies['TICKER'] == ticker].reset_index(drop=True)
df_finwiz_stock_data = get_finwiz_stock_data(ticker)
df_stockrow_data = get_stockrow_stock_data(ticker, debug)

import pdb; pdb.set_trace()
url_yf_asset_profile = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/%s?modules=summaryProfile" % (ticker) #sector, industry, website, business summary
json_yf_asset_profile = json.loads(get_page(url_yf_asset_profile).content)
url_yf_financial_data = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/%s?modules=financialData" % (ticker) #last price, target price
json_yf_financial_data = json.loads(get_page(url_yf_financial_data).content)

df_yf_key_statistics = get_yf_key_stats(ticker)

# Get FMPCloud data for company company peers and company earnings surprises
#url_company_profile = "https://fmpcloud.io/api/v3/profile/%s?apikey=%s" % (ticker,fmpcloud_account_key)
url_company_peers = "https://fmpcloud.io/api/v4/stock_peers?symbol=%s&apikey=%s"  % (ticker,fmpcloud_account_key)
json_fmpcloud_company_peers = json.loads(get_page(url_company_peers).content)

url_company_earnings_surprises = "https://fmpcloud.io/api/v3/earnings-surpises/%s?apikey=%s"  % (ticker,fmpcloud_account_key)
json_fmpcloud_earnings_surprises = json.loads(get_page(url_company_earnings_surprises).content)

#url_company_ratios = "https://fmpcloud.io/api/v3/ratios/%s?limit=40&apikey=%s" % (ticker,fmpcloud_account_key)
#url_company_key_metrics_ttm = "https://fmpcloud.io/api/v3/key-metrics-ttm/%s?limit=40&apikey=%s" % (ticker,fmpcloud_account_key)
import pdb; pdb.set_trace()
# Get FMPCloud data for company company peers and company earnings surprises
#TODO: Retrieve company peers metrics

#Download SEC Filings
url_company_sec_filings = "https://fmpcloud.io/api/v3/financial-statements/%s?datatype=zip&apikey=%s" % (ticker,fmpcloud_account_key)
#import pdb; pdb.set_trace()
save_file_name = '/CompanySECFilings/%s.zip' % (ticker)
save_file_directory = '/CompanySECFilings/%s' % (ticker)
download_file(url_company_sec_filings, save_file_name)
unzip_file(save_file_directory,save_file_name)

#Now that we have retrieved all the data, lets start writing them to the excel template
#Excel file where we will create our one pager
excel_file_path = '/Trading_Excel_Files/04_Filtering_Process/030_Filtering_Process_Quantitative_Analysis_US_StocksOnePager.xlsm'

sheet_name = ticker
sheet_template = "Template"

#If sheet with ticker name does not exist, create one using the template
if not check_sheet_exists(excel_file_path,sheet_name):
    create_sheet(excel_file_path, sheet_name, sheet_template)

#TODO: Populate ticker sheet with company and stock data
##Ticker
row = 2
column = 2
value = ticker
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

##Last
row = 4
column = 3
value = json_yf_financial_data['quoteSummary']['result'][0]['financialData']['currentPrice']['raw']
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

##52 Week High
row = 6
column = 3
value = df_zacks_stock_data['52_WEEK_HIGH'].values[0]
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

##52 Week Low
row = 7
column = 3
value = df_zacks_stock_data['52_WEEK_LOW'].values[0]
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

##YTD Change %
row = 8
column = 3
value = df_zacks_stock_data['PERCENT_PRICE_CHANGE_YTD'].values[0]/100
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

##Mkt Cap
row = 9
column = 3
value = df_zacks_stock_data['MARKET_CAP'].values[0]
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

#TODO: EV, Days to Cover

##Target Price
row = 12
column = 3
value = json_yf_financial_data['quoteSummary']['result'][0]['financialData']['targetMeanPrice']['raw']
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

##Trailing PE
row = 6
column = 6
value = df_zacks_stock_data['PE_TTM'].values[0]
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

##Forward PE
row = 7
column = 6
value = df_zacks_stock_data['PE_F1'].values[0]
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

##PEG
row = 8
column = 6
value = df_zacks_stock_data['PEG_RATIO'].values[0]
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

#TODO: Dividend TTM, Div. yield

##Beta
row = 11
column = 6
value = df_zacks_stock_data['BETA'].values[0]
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

##Company Name
row = 2
column = 7
value = df_zacks_stock_data['COMPANY_NAME'].values[0]
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

#TODO: PB

##ROE
row = 7
column = 8
value = json_yf_financial_data['quoteSummary']['result'][0]['financialData']['returnOnEquity']['raw']
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

##Exchange
row = 8
column = 8
value = df_zacks_stock_data['EXCHANGE'].values[0]
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

##Sector
row = 9
column = 8
value = json_yf_asset_profile['quoteSummary']['result'][0]['summaryProfile']['sector']
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

##Industry
row = 10
column = 8
value = json_yf_asset_profile['quoteSummary']['result'][0]['summaryProfile']['industry']
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

##Website
row = 11
column = 8
value = json_yf_asset_profile['quoteSummary']['result'][0]['summaryProfile']['website']
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

##Company Description
row = 44
column = 10
value = json_yf_asset_profile['quoteSummary']['result'][0]['summaryProfile']['longBusinessSummary']
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

##Fiscal Year End
row = 14
column = 8
value = df_zacks_stock_data['MONTH_OF_FISCAL_YR_END'].values[0]
value = date(1900, value, 1).strftime('%B')
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

# Select the ones you want
df_historical_data = df_stockrow_data[[0]]

print("Done!")

"""
Company:
Sales
EBITDA
Operating Profit (EBIT)
Net income
P/E ratio
EPS
Debt /EBITDA
Cash Flow per share
Book Value per share

Competition:
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


# Company Profile: https://finance.yahoo.com/quote/CRM/profile?p=CRM
# Company Profile: https://www.marketwatch.com/investing/stock/crm/company-profile
# Competitors: https://www.marketwatch.com/investing/stock/crm

#fmpcloud urls:
url_company_profile = "https://fmpcloud.io/api/v3/profile/%s?apikey=%s" % (ticker,fmpcloud_account_key)
#url_company_key_metrics = "https://fmpcloud.io/api/v3/key-metrics-ttm/%s?limit=40&apikey=%s"  % (ticker,fmpcloud_account_key)
url_company_peers = "https://fmpcloud.io/api/v4/stock_peers?symbol=%s&apikey=%s"  % (ticker,fmpcloud_account_key)
url_company_earnings_surprises = "https://fmpcloud.io/api/v3/earnings-surpises/%s?apikey=%s"  % (ticker,fmpcloud_account_key)
url_company_sec_filings = "https://fmpcloud.io/api/v3/financial-statements/%s?datatype=zip&apikey=%s" % (ticker,fmpcloud_account_key)
#url_company_ratios = "https://fmpcloud.io/api/v3/ratios/%s?limit=40&apikey=%s" % (ticker,fmpcloud_account_key)
#url_company_income_statement = "https://fmpcloud.io/api/v3/income-statement/%s?limit=120&apikey=%s" % (ticker,fmpcloud_account_key)
#url_company_balance_sheet = "https://fmpcloud.io/api/v3/balance-sheet-statement/%s?limit=120&apikey=%s" % (ticker,fmpcloud_account_key)
#url_company_cash_flow_statement = "https://fmpcloud.io/api/v3/cash-flow-statement/%s?limit=120&apikey=1%s" % (ticker,fmpcloud_account_key)
#url_company_financial_growth = "https://fmpcloud.io/api/v3/financial-growth/%s?limit=20&apikey=%s" % (ticker,fmpcloud_account_key)


#print(df_zacks_stock_data)
#print(df_finwiz_stock_data)
#print(df_nasdaq_company_data)


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

#nasdaq urls:
url = "https://data.nasdaq.com/api/v3/datatables/ZACKS/EE.json?api_key=%s" % (nasdaq_data_api_key)
data_earnings_estimates = get_api_json_data_no_file(url)

url = "https://data.nasdaq.com/api/v3/datatables/ZACKS/SE.json?api_key=%s" % (nasdaq_data_api_key)
data_sales_estimates = get_api_json_data_no_file(url)

url = "https://data.nasdaq.com/api/v3/datatables/ZACKS/FC.json?api_key=%s" % (nasdaq_data_api_key)
data_fundamentals = get_api_json_data_no_file(url)

url = "https://data.nasdaq.com/api/v3/datatables/ZACKS/ES.json?api_key=%s" % (nasdaq_data_api_key)
data_earnings_surprises = get_api_json_data_no_file(url)

def return_tr_as_df(table_rows):
    df = pd.DataFrame()
    index = 0
    #Get rows of data.
    for tr in table_rows:

        key = tr.th.text.strip()
        try:
            value = tr.td.text.strip()
        except AttributeError as e:
            value = ""

        if(value):
            df.insert(index,key,[value],True)
            index+=1
    return df

if not debug:
    url_nasdaq = "https://www.nasdaq.com/market-activity/stocks/%s/price-earnings-peg-ratios" % (ticker)
    page = get_page_selenium(url_nasdaq)
    soup = BeautifulSoup(page, 'html.parser')

    table = soup.find_all('table')
    pe_ratio_table_rows = table[0].find_all('tr', recursive=True)

    df_pe_ratios = return_tr_as_df(pe_ratio_table_rows)

    df_nasdaq_company_data = pd.DataFrame()
    df_nasdaq_company_data.loc[ticker, 'PE_F0-1_ACTUAL'] = df_pe_ratios.iloc[0,0]
    df_nasdaq_company_data.loc[ticker, 'PE_F0_ESTIMATE'] = df_pe_ratios.iloc[0,1]
    df_nasdaq_company_data.loc[ticker, 'PE_F1_ESTIMATE'] = df_pe_ratios.iloc[0,2]
    df_nasdaq_company_data.loc[ticker, 'PE_F2_ESTIMATE'] = df_pe_ratios.iloc[0,3]

    df_nasdaq_company_data = df_nasdaq_company_data.reset_index()
    df_nasdaq_company_data = df_nasdaq_company_data.rename(columns = {'index':'TICKER'})

    df_nasdaq_company_data['PE_F0-1_ACTUAL'] = pd.to_numeric(df_nasdaq_company_data['PE_F0-1_ACTUAL'])
    df_nasdaq_company_data['PE_F0_ESTIMATE'] = pd.to_numeric(df_nasdaq_company_data['PE_F0_ESTIMATE'])
    df_nasdaq_company_data['PE_F1_ESTIMATE'] = pd.to_numeric(df_nasdaq_company_data['PE_F1_ESTIMATE'])
    df_nasdaq_company_data['PE_F2_ESTIMATE'] = pd.to_numeric(df_nasdaq_company_data['PE_F2_ESTIMATE'])
else:
    #hard code values in debug mode so that we don't spend time waiting for selenium to load page
    data = [['AAPL',24.44,22.44,20.65,19.31]]
    df_nasdaq_company_data = pd.DataFrame(data, columns=['TICKER','PE_F0-1_ACTUAL','PE_F0_ESTIMATE','PE_F1_ESTIMATE','PE_F2_ESTIMATE'])
"""