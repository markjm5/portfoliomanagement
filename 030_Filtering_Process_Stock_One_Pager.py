import pandas as pd
from pandas.tseries.offsets import BDay
from datetime import date
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
from common import get_oecd_data, convert_excelsheet_to_dataframe, write_dataframe_to_excel
from common import get_api_json_data,combine_df_on_index, write_value_to_cell_excel

excel_file_path = '/Trading_Excel_Files/04_Filtering_Process/030_Filtering_Process_Quantitative_Analysis_Stock_One_Page.xlsm'

fmpcloud_account_key = '14afe305132a682a2742743df532707d'
nasdaq_data_api_key = "u4udsfUDYFey58cp_4Gg"

todays_date = date.today()
one_year_ago = dt(todays_date.year - 1, 12, 31)
two_year_ago = dt(todays_date.year - 2, 12, 31)
three_year_ago = dt(todays_date.year - 3, 12, 31)

list_dates = []
list_dates.append(one_year_ago)
list_dates.append(two_year_ago)
list_dates.append(three_year_ago)

#########################
# Get S&P500 Last Price #
#########################
"""
sheet_name = 'Data S&P 500'

url = "https://fmpcloud.io/api/v3/quotes/index?apikey=%s" % (fmpcloud_account_key)
data_sp_price = get_api_json_data(url,'030_SP500_details.json')
sp_price = ""
for index in data_sp_price:
    if index['symbol'] == '^GSPC':
        sp_price = index['price']
row = 4
column = 4
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, sp_price)

#################################
# Get Aggregate Data for S&P500 #
#################################

sheet_name = 'Database S&P500'
#df_sp_500 = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

url = "https://data.nasdaq.com/api/v3/datasets/MULTPL/SP500_EARNINGS_YEAR.json?api_key=%s" % (nasdaq_data_api_key)
data_sp_earnings = get_api_json_data(url,'030_SP500_earnings.json')

url = "https://data.nasdaq.com/api/v3/datasets/MULTPL/SP500_DIV_YIELD_MONTH.json?api_key=%s" % (nasdaq_data_api_key)
data_sp_dividend_yield = get_api_json_data(url,'030_SP500_dividend_yield.json')

url = "https://data.nasdaq.com/api/v3/datasets/MULTPL/SP500_PE_RATIO_YEAR.json?api_key=%s" % (nasdaq_data_api_key)
data_sp_earnings_ratio = get_api_json_data(url,'030_SP500_price_to_earnings_ratio.json')

url = "https://data.nasdaq.com/api/v3/datasets/MULTPL/SP500_PSR_YEAR.json?api_key=%s" % (nasdaq_data_api_key)
data_sp_price_to_sales_ratio = get_api_json_data(url,'030_SP500_price_to_sales_ratio.json')

df_sp_earnings = pd.DataFrame()
df_sp_dividend_yield = pd.DataFrame()
df_sp_earnings_ratio = pd.DataFrame()
df_sp_price_to_sales_ratio = pd.DataFrame()

for index in data_sp_earnings['dataset']['data']:   
    df_sp_earnings = df_sp_earnings.append({"DATE": dt.strptime(index[0],"%Y-%m-%d"), "EPS": index[1]}, ignore_index=True)

for index in data_sp_dividend_yield['dataset']['data']:   
    df_sp_dividend_yield = df_sp_dividend_yield.append({"DATE": dt.strptime(index[0],"%Y-%m-%d"), "DIVIDEND_YIELD": index[1]}, ignore_index=True)

for index in data_sp_earnings_ratio['dataset']['data']:   
    df_sp_earnings_ratio = df_sp_earnings_ratio.append({"DATE": dt.strptime(index[0],"%Y-%m-%d"), "PE_RATIO": index[1]}, ignore_index=True)

for index in data_sp_price_to_sales_ratio['dataset']['data']:   
    df_sp_price_to_sales_ratio = df_sp_price_to_sales_ratio.append({"DATE": dt.strptime(index[0],"%Y-%m-%d"), "PRICE_SALES_RATIO": index[1]}, ignore_index=True)

df_history = combine_df_on_index(df_sp_dividend_yield, df_sp_earnings,'DATE')
df_history = combine_df_on_index(df_history, df_sp_earnings_ratio,'DATE')
df_history = combine_df_on_index(df_history, df_sp_price_to_sales_ratio,'DATE')

df_current = df_history.tail(1)

df_history = df_history.loc[df_history['DATE'].isin(list_dates)]
df_history = df_history.reset_index(drop=True)

df_updated = combine_df_on_index(df_history, df_current,'DATE')

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

#TODO:
# Earnings Per Share (Annual)
# Price to Earnings (P/E) (Annual)*
# Dividend Yield*	
# Book Value per share
# Calculate Price to Book (P/B)
# Calculate Price to Sales (P/S) *
"""
##################################
# Get Aggregate Data for Sectors #
##################################

sheet_name = 'Database Sectors'

last_business_day = todays_date - BDay(1)
todays_date_formatted = last_business_day.strftime("%Y-%m-%d")

# Sectors PE Ratio: https://fmpcloud.io/api/v4/sector_price_earning_ratio?date=2021-05-07&exchange=NYSE&apikey=14afe305132a682a2742743df532707d
url = "https://fmpcloud.io/api/v4/sector_price_earning_ratio?date=%s&exchange=NYSE&apikey=%s" % (todays_date_formatted, fmpcloud_account_key)
data_sector_pe_ratio = get_api_json_data(url,'030_sector_pe_ratio.json')

df_sector_pe_ratio = pd.DataFrame()

#TODO: Read data from json files, and put them into dataframes and write them to excel

for index in data_sector_pe_ratio:   
    df_sector_pe_ratio = df_sector_pe_ratio.append({"DATE": dt.strptime(index['date'],"%Y-%m-%d"), "SECTOR": index['sector'],"PE": pd.to_numeric(index['pe'])}, ignore_index=True)

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_sector_pe_ratio, False, 0)

#TODO: Convert the above json files into a dataframe
#####################################
# Get Aggregate Data for Industries #
#####################################
sheet_name = 'Database Industries'

# Industries PE Ratio: https://fmpcloud.io/api/v4/industry_price_earning_ratio?date=2021-05-07&exchange=NYSE&apikey=14afe305132a682a2742743df532707d
url = "https://fmpcloud.io/api/v4/industry_price_earning_ratio?date=%s&exchange=NYSE&apikey=%s" % (todays_date_formatted, fmpcloud_account_key)
data_industry_pe_ratio = get_api_json_data(url,'030_industry_pe_ratio.json')

df_industry_pe_ratio = pd.DataFrame()

for index in data_industry_pe_ratio:   
    df_industry_pe_ratio = df_industry_pe_ratio.append({"DATE": dt.strptime(index['date'],"%Y-%m-%d"), "INDUSTRY": index['industry'],"PE": pd.to_numeric(index['pe'])}, ignore_index=True)

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_industry_pe_ratio, False, 0)

################################################
# Get Aggregate Data for Single Name Companies #
################################################

sheet_name = 'Database US Companies'
df_us_companies = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

import pdb; pdb.set_trace()

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
