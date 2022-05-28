import pandas as pd
from datetime import date
from common import get_oecd_data, convert_excelsheet_to_dataframe, write_dataframe_to_excel
from common import get_fmpcloud_data

excel_file_path = '/Trading_Excel_Files/04_Filtering_Process/030_Filtering_Process_Quantitative_Analysis_Stock_One_Page.xlsm'

fmpcloud_account_key = '14afe305132a682a2742743df532707d'

#################################
# Get Aggregate Data for S&P500 #
#################################

sheet_name = 'Database S&P500'
df_sp_500 = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

url = "https://fmpcloud.io/api/v3/quotes/index?apikey=%s" % (fmpcloud_account_key)

sp_index = '^GSPC'

data = get_fmpcloud_data(url,'030_SP500_details.json')

sp_price = ""
for index in data:
    if index['symbol'] == '^GSPC':
        sp_price = index['price']

print(sp_price)

#TODO:
# Earnings Per Share (Annual)
# Price to Earnings (P/E) (Annual)
# Dividend Yield	
# Book Value per share
# Calculate Price to Book (P/B)
# Calculate Price to Sales (P/S)
import pdb; pdb.set_trace()
#################################################
# Get Aggregate Data for Sectors and Industries #
#################################################

# Sectors PE Ratio: https://fmpcloud.io/api/v4/sector_price_earning_ratio?date=2021-05-07&exchange=NYSE&apikey=14afe305132a682a2742743df532707d
# Industries PE Ratio: https://fmpcloud.io/api/v4/industry_price_earning_ratio?date=2021-05-07&exchange=NYSE&apikey=14afe305132a682a2742743df532707d

################################################
# Get Aggregate Data for Single Name Companies #
################################################


sheet_name = 'Database US Companies'
df_us_companies = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

import pdb; pdb.set_trace()

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
