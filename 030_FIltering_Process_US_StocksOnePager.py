import pandas as pd
from pandas.tseries.offsets import BDay
from bs4 import BeautifulSoup
from datetime import date
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
from common import get_oecd_data, convert_excelsheet_to_dataframe, write_dataframe_to_excel
from common import get_api_json_data,get_page, get_finwiz_stock_data, get_api_json_data_no_file, get_page_selenium, combine_df_on_index, write_value_to_cell_excel

#Sources:
#https://finance.yahoo.com/
#https://www.reuters.com
#https://www.marketwatch.com/
#https://www.marketscreener.com/
#https://stockrow.com/

#################
ticker = "AAPL" # COMPANY TICKER - CHANGE HERE
#################

fmpcloud_account_key = '14afe305132a682a2742743df532707d'
nasdaq_data_api_key = "u4udsfUDYFey58cp_4Gg"

#Dates
todays_date = date.today()
one_year_ago = dt(todays_date.year - 1, 12, 31)
two_year_ago = dt(todays_date.year - 2, 12, 31)
three_year_ago = dt(todays_date.year - 3, 12, 31)
list_dates = []
list_dates.append(one_year_ago)
list_dates.append(two_year_ago)
list_dates.append(three_year_ago)

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

temp_excel_file_path = '/Trading_Excel_Files/04_Filtering_Process/030_Filtering_Process_Quantitative_Analysis_US_Stocks.xlsm'
temp_sheet_name = 'Database US Companies'

#Get company data from various sources
df_us_companies = convert_excelsheet_to_dataframe(temp_excel_file_path, temp_sheet_name, False)
df_zacks_stock_data = df_company_details = df_us_companies.loc[df_us_companies['TICKER'] == ticker].reset_index(drop=True)
df_finwiz_stock_data = get_finwiz_stock_data(ticker)

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

#fmpcloud urls:
url_company_profile = "https://fmpcloud.io/api/v3/profile/%s?apikey=%s" % (ticker,fmpcloud_account_key)
url_company_key_metrics = "https://fmpcloud.io/api/v3/key-metrics-ttm/%s?limit=40&apikey=%s"  % (ticker,fmpcloud_account_key)
url_company_peers = "https://fmpcloud.io/api/v4/stock_peers?symbol=%s&apikey=%s"  % (ticker,fmpcloud_account_key)
url_company_earnings_surprises = "https://fmpcloud.io/api/v3/earnings-surpises/%s?apikey=%s"  % (ticker,fmpcloud_account_key)
url_company_sec_filings = "https://fmpcloud.io/api/v3/financial-statements/%s?datatype=zip&apikey=%s" % (ticker,fmpcloud_account_key)
url_company_ratios = "https://fmpcloud.io/api/v3/ratios/%s?limit=40&apikey=%s" % (ticker,fmpcloud_account_key)
url_company_income_statement = "https://fmpcloud.io/api/v3/income-statement/%s?limit=120&apikey=%s" % (ticker,fmpcloud_account_key)
url_company_balance_sheet = "https://fmpcloud.io/api/v3/balance-sheet-statement/%s?limit=120&apikey=%s" % (ticker,fmpcloud_account_key)
url_company_cash_flow_statement = "https://fmpcloud.io/api/v3/cash-flow-statement/%s?limit=120&apikey=1%s" % (ticker,fmpcloud_account_key)
url_company_financial_growth = "https://fmpcloud.io/api/v3/financial-growth/%s?limit=20&apikey=%s" % (ticker,fmpcloud_account_key)

"""
#nasdaq urls:
url = "https://data.nasdaq.com/api/v3/datatables/ZACKS/EE.json?api_key=%s" % (nasdaq_data_api_key)
data_earnings_estimates = get_api_json_data_no_file(url)

url = "https://data.nasdaq.com/api/v3/datatables/ZACKS/SE.json?api_key=%s" % (nasdaq_data_api_key)
data_sales_estimates = get_api_json_data_no_file(url)

url = "https://data.nasdaq.com/api/v3/datatables/ZACKS/FC.json?api_key=%s" % (nasdaq_data_api_key)
data_fundamentals = get_api_json_data_no_file(url)

url = "https://data.nasdaq.com/api/v3/datatables/ZACKS/ES.json?api_key=%s" % (nasdaq_data_api_key)
data_earnings_surprises = get_api_json_data_no_file(url)
"""

#Excel file where we will create our one pager
excel_file_path = '/Trading_Excel_Files/04_Filtering_Process/030_Filtering_Process_Quantitative_Analysis_US_StocksOnePager.xlsm'

print(df_zacks_stock_data)
print(df_finwiz_stock_data)
print(df_nasdaq_company_data)

import pdb; pdb.set_trace()

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
