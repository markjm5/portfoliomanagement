from json.encoder import py_encode_basestring
from numpy import subtract
import pandas as pd
import json
from datetime import date
from datetime import datetime as dt
from common import convert_excelsheet_to_dataframe, get_stockrow_stock_data
from common import get_page, get_finwiz_stock_data, get_stockrow_stock_data, get_zacks_balance_sheet_shares
from common import get_zacks_peer_comparison, get_zacks_earnings_surprises, get_zacks_product_line_geography
from common import write_value_to_cell_excel, check_sheet_exists, create_sheet
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

# TODO: Get Share Buyback for Quarter: https://query2.finance.yahoo.com/v10/finance/quoteSummary/AAPL?modules=cashflowStatementHistoryQuarterly
# TODO: Get Share Buyback For Year: https://query2.finance.yahoo.com/v10/finance/quoteSummary/AAPL?modules=cashflowStatementHistory

# TODO: Get Historical Quarterly Revenue Actual: https://query2.finance.yahoo.com/v10/finance/quoteSummary/AAPL?modules=incomeStatementHistoryQuarterly
# TODO: Get Historical Quarterly Revenue Estimates: https://query2.finance.yahoo.com/v10/finance/quoteSummary/AAPL?modules=earningsTrend

# TODO: Get Historical Quarterly EPS Actual + Estimates: https://query2.finance.yahoo.com/v10/finance/quoteSummary/AAPL?modules=earningsHistory

debug = True

#################
ticker = "AAPL" # COMPANY TICKER - CHANGE HERE
#################

#Dates
todays_date = date.today()
one_year_ago = dt(todays_date.year - 1, 12, 31)
two_year_ago = dt(todays_date.year - 2, 12, 31)
three_year_ago = dt(todays_date.year - 3, 12, 31)
list_dates = []
list_dates.append(one_year_ago)
list_dates.append(two_year_ago)
list_dates.append(three_year_ago)

def write_multiple_value(row, column_start, col_name):
    # Historical and Estimated data

    #TODO: Rather than getting last 6 columns, look at current year, and get +2 projections, and -3 historical

    df = df_stockrow_data.iloc[-6:][col_name]
    df = df.to_frame()
    df = df.T

    for column in df:
        value = df[column].values[0]
        write_value_to_cell_excel(excel_file_path,sheet_name, row, column_start, value)
        column_start = column_start+1

fmpcloud_account_key = '14afe305132a682a2742743df532707d'

temp_excel_file_path = '/Trading_Excel_Files/04_Filtering_Process/030_Filtering_Process_Quantitative_Analysis_US_Stocks.xlsm'
temp_sheet_name = 'Database US Companies'

#Get company data from various sources
df_us_companies = convert_excelsheet_to_dataframe(temp_excel_file_path, temp_sheet_name, False)
df_zacks_stock_data = df_us_companies.loc[df_us_companies['TICKER'] == ticker].reset_index(drop=True)
df_zacks_balance_sheet_shares_annual, df_zacks_balance_sheet_shares_quarterly = get_zacks_balance_sheet_shares(ticker)
df_zacks_peer_comparison = get_zacks_peer_comparison(ticker)
df_zacks_next_earnings_release, df_zacks_earnings_surprises = get_zacks_earnings_surprises(ticker)
df_zacks_product_line, df_zacks_geography = get_zacks_product_line_geography(ticker)
df_finwiz_stock_data = get_finwiz_stock_data(ticker)
df_stockrow_data = get_stockrow_stock_data(ticker, debug)
#df_wsj_ebitda = get_yf_analysis(ticker)

url_yf_modules = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/%s?modules=summaryProfile,financialData,summaryDetail,price,defaultKeyStatistics" % (ticker)
json_yf_modules = json.loads(get_page(url_yf_modules).content)

df_yf_key_statistics = get_yf_key_stats(ticker)

#url_company_ratios = "https://fmpcloud.io/api/v3/ratios/%s?limit=40&apikey=%s" % (ticker,fmpcloud_account_key)
#url_company_key_metrics_ttm = "https://fmpcloud.io/api/v3/key-metrics-ttm/%s?limit=40&apikey=%s" % (ticker,fmpcloud_account_key)

df_peer_metrics = pd.DataFrame(columns=['TICKER','MARKET_CAP','EV','PE','EV_EBITDA','EV_EBIT','EV_REVENUE','PB','EBITDA_MARGIN','EBIT_MARGIN','NET_MARGIN','DIVIDEND_YIELD','ROE'])
#Retrieve company peers metrics
for row,peer in df_zacks_peer_comparison.iterrows():
    temp_row = []
    peer_ticker = peer[1]
    df_peer_zacks_stock_data = df_us_companies.loc[df_us_companies['TICKER'] == peer_ticker].reset_index(drop=True)
    if(len(df_peer_zacks_stock_data) > 0):

        peer_market_cap = df_peer_zacks_stock_data['MARKET_CAP'].values[0]

        #Calculate EV
        peer_current_assets = df_peer_zacks_stock_data['CURRENT_ASSETS(MILLION)']
        peer_current_liabilities = df_peer_zacks_stock_data['CURRENT_LIABILITIES(MILLION)']	
        peer_long_term_debt = df_peer_zacks_stock_data['LONG_TERM_DEBT(MILLION)']

        try:
            peer_ev = round(peer_market_cap + ((peer_current_liabilities + peer_long_term_debt) - peer_current_assets),2)            

        except ArithmeticError:
            peer_ev = 0

        peer_pe = df_peer_zacks_stock_data['PE_TTM'].values[0]

        try:
            peer_ev_ebitda = round(peer_ev/df_peer_zacks_stock_data['EBITDA_MIL'].values[0],2)
        except ArithmeticError:
            peer_ev_ebitda = 0

        try:
            peer_ev_ebit = round(peer_ev/df_peer_zacks_stock_data['EBIT_MIL'].values[0],2)
        except ArithmeticError:
            peer_ev_ebit = 0

        try:
            peer_ev_revenue = round(peer_ev/df_peer_zacks_stock_data['ANNUAL_SALES(MILLION)'].values[0],2)
        except ArithmeticError:
            peer_ev_revenue = 0

        peer_pb = df_peer_zacks_stock_data['PRICE_BOOK_RATIO'].values[0]
        peer_ebitda_margin = 0 # EBITDA margin - Can be calculated using EBITDA?
        peer_ebit_margin = 0 # EBIT margin - Can be calculated using EBIT?
        peer_net_margin = df_peer_zacks_stock_data['NET_MARGIN_PERCENTAGE'].values[0]
        peer_dividend_yield = df_peer_zacks_stock_data['DIVIDEND_YIELD_PERCENTAGE'].values[0]
        peer_roe = df_peer_zacks_stock_data['CURRENT_ROE_TTM'].values[0] 

        temp_row.append(peer_ticker)        
        temp_row.append(peer_market_cap)        
        temp_row.append(peer_ev) 
        temp_row.append(peer_pe)
        temp_row.append(peer_ev_ebitda)
        temp_row.append(peer_ev_ebit)
        temp_row.append(peer_ev_revenue)
        temp_row.append(peer_pb)
        temp_row.append(peer_ebitda_margin)
        temp_row.append(peer_ebit_margin)
        temp_row.append(peer_net_margin)
        temp_row.append(peer_dividend_yield)
        temp_row.append(peer_roe) 

        #Add row to dataframe
        df_peer_metrics.loc[len(df_peer_metrics.index)] = temp_row   

#TODO: Format dataframe
import pdb; pdb.set_trace()
"""
#Download SEC Filings from FMPCLOUD
url_company_sec_filings = "https://fmpcloud.io/api/v3/financial-statements/%s?datatype=zip&apikey=%s" % (ticker,fmpcloud_account_key)

save_file_name = '/CompanySECFilings/%s.zip' % (ticker)
save_file_directory = '/CompanySECFilings/%s' % (ticker)
download_file(url_company_sec_filings, save_file_name)
unzip_file(save_file_directory,save_file_name)
"""
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
value = json_yf_modules['quoteSummary']['result'][0]['financialData']['currentPrice']['raw']
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
value = json_yf_modules['quoteSummary']['result'][0]['price']['marketCap']['raw']
value = int(str(value)[:-6])
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

#EV
row = 10
column = 3
value = json_yf_modules['quoteSummary']['result'][0]['defaultKeyStatistics']['enterpriseValue']['raw']
value = int(str(value)[:-6])
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

#Days to Cover (short ratio)
row = 11
column = 3
value = json_yf_modules['quoteSummary']['result'][0]['defaultKeyStatistics']['shortRatio']['raw']
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

##Target Price
row = 12
column = 3
value = json_yf_modules['quoteSummary']['result'][0]['financialData']['targetMeanPrice']['raw']
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
row = 9
column = 5
value = "Dividend %s" % (todays_date.year)
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

row = 9
column = 6
value = json_yf_modules['quoteSummary']['result'][0]['summaryDetail']['trailingAnnualDividendRate']['raw']
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

##Beta
row = 11
column = 6
value = df_zacks_stock_data['BETA'].values[0]
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

##Company Name
row = 2
column = 7
value = json_yf_modules['quoteSummary']['result'][0]['price']['longName']
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

##ROE
row = 7
column = 8
value = json_yf_modules['quoteSummary']['result'][0]['financialData']['returnOnEquity']['raw']
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

##Exchange
row = 8
column = 8
value = df_zacks_stock_data['EXCHANGE'].values[0]
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

##Sector
row = 9
column = 8
value = json_yf_modules['quoteSummary']['result'][0]['summaryProfile']['sector']
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

##Industry
row = 10
column = 8
value = json_yf_modules['quoteSummary']['result'][0]['summaryProfile']['industry']
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

##Website
row = 11
column = 8
value = json_yf_modules['quoteSummary']['result'][0]['summaryProfile']['website']
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

##Company Description
row = 44
column = 12
value = json_yf_modules['quoteSummary']['result'][0]['summaryProfile']['longBusinessSummary']
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

##Fiscal Year End
row = 14
column = 8
value = df_zacks_stock_data['MONTH_OF_FISCAL_YR_END'].values[0]
value = date(1900, value, 1).strftime('%B')
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

# Historical and Estimated Sales
df_historical_sales = df_stockrow_data.iloc[-6:]['SALES']
df_historical_sales = df_historical_sales.to_frame()
df_historical_sales = df_historical_sales.T

column_start = 3
for column in df_historical_sales:
    value1 = int(column)
    value2 = df_historical_sales[column].values[0]
    write_value_to_cell_excel(excel_file_path,sheet_name, 15, column_start, value1)
    write_value_to_cell_excel(excel_file_path,sheet_name, 16, column_start, value2)
    column_start = column_start+1

# Historical and Estimated EBITDA
row = 18
column_start = 3
col_name = 'EBITDA'
write_multiple_value(row, column_start, col_name)

# Historical and Estimated EBIT
row = 20
column_start = 3
col_name = 'EBIT'
write_multiple_value(row, column_start, col_name)

row = 22
column_start = 3
col_name = 'NET_INCOME'
write_multiple_value(row, column_start, col_name)

# Historical and Estimated PE Ratio
row = 25
column_start = 3
col_name = 'PE_RATIO'
write_multiple_value(row, column_start, col_name)

# Historical and Estimated Earnings Per Share
row = 26
column_start = 3
col_name = 'EARNINGS_PER_SHARE'
write_multiple_value(row, column_start, col_name)

# Historical and Estimated Total Debt
row = 32
column_start = 3
col_name = 'TOTAL_DEBT'
write_multiple_value(row, column_start, col_name)

# Historical and Estimated Cash Flow Per Share
row = 35
column_start = 3
col_name = 'CASH_FLOW_PER_SHARE'
write_multiple_value(row, column_start, col_name)

# Historical and Estimated Book Value Per Share
row = 36
column_start = 3
col_name = 'BOOK_VALUE_PER_SHARE'
write_multiple_value(row, column_start, col_name)

## Average Volume 10 days
row = 38
column = 3
value = json_yf_modules['quoteSummary']['result'][0]['summaryDetail']['volume']['raw']
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

## Average Volume 10 days
row = 39
column = 3
value = df_yf_key_statistics['AVG_VOL_10D'].values[0]
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

## Average Volume 3 months
row = 40
column = 3
value = df_yf_key_statistics['AVG_VOL_3M'].values[0]
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

## 50 Day Moving Average
row = 38
column = 7
value = df_yf_key_statistics['50_DAY_MOVING_AVG'].values[0]
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

## 50 Day Moving Average
row = 39
column = 7
value = df_yf_key_statistics['200_DAY_MOVING_AVG'].values[0]
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)


## 50 Day Moving Average
row = 39
column = 7
value = df_yf_key_statistics['200_DAY_MOVING_AVG'].values[0]
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

## Buyback Year
row = 41
column = 8
value = df_zacks_balance_sheet_shares_annual.iloc[0]['TREASURY_STOCK']
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

## Buyback Quarter
row = 42
column = 8
value = df_zacks_balance_sheet_shares_quarterly.iloc[0]['TREASURY_STOCK']
write_value_to_cell_excel(excel_file_path,sheet_name, row, column, value)

# Sales Per Region
row_start = 44
column1 = 3
column2 = 4

for index, row in df_zacks_geography.iterrows():
    value1 = row[0]
    value2 = row[1]
    write_value_to_cell_excel(excel_file_path,sheet_name, row_start, column1, value1)
    write_value_to_cell_excel(excel_file_path,sheet_name, row_start, column2, value2)
    row_start = row_start+1

# Sales Per Product Line
#df_zacks_product_line
row_start = 44
column1 = 8
column2 = 9

for index, row in df_zacks_product_line.iterrows():
    value1 = row[0]
    value2 = row[1]
    write_value_to_cell_excel(excel_file_path,sheet_name, row_start, column1, value1)
    write_value_to_cell_excel(excel_file_path,sheet_name, row_start, column2, value2)
    row_start = row_start+1

# Competitors
#df_zacks_peer_comparison
row_start = 7
column1 = 11
column2 = 12

for index, row in df_zacks_peer_comparison.iterrows():
    value1 = row[0]
    value2 = row[1]
    write_value_to_cell_excel(excel_file_path,sheet_name, row_start, column1, value1)
    write_value_to_cell_excel(excel_file_path,sheet_name, row_start, column2, value2)
    row_start = row_start+1

# Competitor Metrics

# Historical Surprises
#df_zacks_earnings_surprises
row_start = 32
column_period = 10
column_estimate = 12
column_reported = 13

for index, row in df_zacks_earnings_surprises.iterrows():
    period = row[1]
    eps_estimate = row[2]
    eps_reported = row[3]
    sales_estimate = row[4]
    sales_reported = row[5]

    write_value_to_cell_excel(excel_file_path,sheet_name, row_start, column_period, period)
    write_value_to_cell_excel(excel_file_path,sheet_name, row_start, column_estimate, sales_estimate)
    write_value_to_cell_excel(excel_file_path,sheet_name, row_start, column_reported, sales_reported)
    write_value_to_cell_excel(excel_file_path,sheet_name, row_start + 1, column_estimate, eps_estimate)
    write_value_to_cell_excel(excel_file_path,sheet_name, row_start + 1, column_reported, eps_reported)

    row_start = row_start+2




# Upcoming Events
#df_zacks_next_earnings_release, 


print("Done!")

"""
# Company Profile: https://finance.yahoo.com/quote/CRM/profile?p=CRM
# Company Profile: https://www.marketwatch.com/investing/stock/crm/company-profile
# Competitors: https://www.marketwatch.com/investing/stock/crm

Historical Earnings Surprises
url_nasdaq = "https://www.nasdaq.com/market-activity/stocks/%s/price-earnings-peg-ratios" % (ticker)
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