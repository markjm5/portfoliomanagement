import pandas as pd
import quandl
from pandas.tseries.offsets import BDay
from bs4 import BeautifulSoup
from datetime import date
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
from common import get_oecd_data, convert_excelsheet_to_dataframe, write_dataframe_to_excel
from common import get_api_json_data,get_page,get_api_json_data_no_file, get_page_selenium, combine_df_on_index, write_value_to_cell_excel

excel_file_path = '/Trading_Excel_Files/04_Filtering_Process/030_Filtering_Process_Quantitative_Analysis_US_Stocks.xlsm'

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

for index in data_sector_pe_ratio:   
    df_sector_pe_ratio = df_sector_pe_ratio.append({"DATE": dt.strptime(index['date'],"%Y-%m-%d"), "SECTOR": index['sector'],"PE": pd.to_numeric(index['pe'])}, ignore_index=True)

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_sector_pe_ratio, False, 0)

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

"""
################################################
# Get Aggregate Data for Single Name Companies #
################################################

sheet_name = 'Database US Companies'
df_us_companies = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)



df_us_companies = df_us_companies.rename(columns={"Company Name": "COMPANY_NAME", "Ticker": "TICKER", "Market Cap (mil)": "MARKET_CAP", "Last EPS Surprise (%)": "LAST_EPS_SURPRISE_PERCENTAGE", "Div. Yield %": "DIVIDEND_YIELD_PERCENTAGE", "Exchange": "EXCHANGE", "Industry": "INDUSTRY", 
"Sector": "SECTOR", "Month of Fiscal Yr End": "MONTH_OF_FISCAL_YR_END","F0 Consensus Est.": "EPS_F0_CONSENSUS","F1 Consensus Est.": "EPS_F1_CONSENSUS",
"F2 Consensus Est.": "EPS_F2_CONSENSUS","P/E (Trailing 12 Months)": "PE_TTM","P/E (F1)": "PE_F1","P/E (F2)": "PE_F2","PEG Ratio": "PEG_RATIO",
"Next EPS Report Date  (yyyymmdd)": "NEXT_EPS_REPORT_DATE","Current ROE (TTM)": "CURRENT_ROE_TTM","5 Yr Historical Sales Growth": "5Y_HISTORICAL_SALES_GROWTH","F(1) Consensus Sales Est. ($mil)": "F1_CONSENSUS_SALES_ESTIMATE","Annual Sales ($mil)": "ANNUAL_SALES(MILLION)",
"Price/Sales": "PRICE_SALES_RATIO", "Price/Book": "PRICE_BOOK_RATIO","Net Margin %": "NET_MARGIN_PERCENTAGE",
"Operating Margin 12 Mo %": "OPERATING_MARGIN_12_MO","Debt/Total Capital": "DEBT_TOTAL_CAPITAL","Debt/Equity Ratio": "DEBT_EQUITY_RATIO","Current Ratio": "CURRENT_RATIO","Quick Ratio": "QUICK_RATIO"
})

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_us_companies, False, 0)

################
# Sales Growth #
################

sheet_name = 'Sales Growth'
#df_original_us_companies_sales_growth = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

df_us_companies_sales_growth = df_us_companies.filter(['COMPANY_NAME','TICKER','SECTOR','INDUSTRY','5Y_HISTORICAL_SALES_GROWTH', 'F1_CONSENSUS_SALES_ESTIMATE', 'ANNUAL_SALES(MILLION)'])
df_us_companies_sales_growth["F1_SALES_GROWTH"] = (df_us_companies_sales_growth['F1_CONSENSUS_SALES_ESTIMATE'] - df_us_companies_sales_growth['ANNUAL_SALES(MILLION)'])/df_us_companies_sales_growth['ANNUAL_SALES(MILLION)']
df_us_companies_sales_growth["5Y_HISTORICAL_SALES_GROWTH"] = df_us_companies_sales_growth['5Y_HISTORICAL_SALES_GROWTH']/100

df_us_companies_sales_growth = df_us_companies_sales_growth.drop(columns='F1_CONSENSUS_SALES_ESTIMATE', axis=1)
df_us_companies_sales_growth = df_us_companies_sales_growth.drop(columns='ANNUAL_SALES(MILLION)', axis=1)

df_us_companies_sales_growth = df_us_companies_sales_growth.rename(columns={"COMPANY_NAME": "Company Name", "TICKER": "Ticker", "SECTOR": "Sector", "INDUSTRY": "Industry", 
"5Y_HISTORICAL_SALES_GROWTH": "5 Yr Historical Sales Growth","F1_SALES_GROWTH": "Sales Growth F1"
})

#Combine df_original_world_gdp with df_world_gdp
#df_updated_us_companies_sales_growth = combine_df_on_index(df_original_us_companies_sales_growth, df_us_companies_sales_growth,'Ticker')

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_us_companies_sales_growth, False, 0)

##############
# EPS Growth #
##############

sheet_name = 'Earnings Growth'

df_us_companies_eps_growth = df_us_companies.filter(['COMPANY_NAME','TICKER','SECTOR','INDUSTRY','EPS_F1_CONSENSUS', 'EPS_F2_CONSENSUS'])

df_us_companies_eps_growth = df_us_companies_eps_growth.rename(columns={"COMPANY_NAME": "Company Name", "TICKER": "Ticker", "SECTOR": "Sector", "INDUSTRY": "Industry", 
"EPS_F1_CONSENSUS": "EPS Growth F1","EPS_F2_CONSENSUS": "EPS Growth F2"
})

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_us_companies_eps_growth, False, 0)

##################
# Dividend Yield #
##################

sheet_name = 'Dividend Yield'

df_us_companies_dividend_yield = df_us_companies.filter(['COMPANY_NAME','TICKER','DIVIDEND_YIELD_PERCENTAGE'])

df_us_companies_dividend_yield = df_us_companies_dividend_yield.rename(columns={"COMPANY_NAME": "Company Name", "TICKER": "Ticker",
"DIVIDEND_YIELD_PERCENTAGE": "Div. Yield %"
})

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_us_companies_dividend_yield, False, 0)

##############
# Net Margin #
##############

sheet_name = 'Margins'

df_us_companies_net_margin = df_us_companies.filter(['COMPANY_NAME','TICKER','OPERATING_MARGIN_12_MO','NET_MARGIN_PERCENTAGE'])

df_us_companies_net_margin = df_us_companies_net_margin.rename(columns={"COMPANY_NAME": "Company Name", "TICKER": "Ticker", 
"OPERATING_MARGIN_12_MO": "Operating Margin 12 Mo %","NET_MARGIN_PERCENTAGE": "Net Margin %"
})

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_us_companies_net_margin, False, 0)

########
# Debt #
########

sheet_name = 'Debt Balance Sheet'

df_us_companies_debt = df_us_companies.filter(['COMPANY_NAME','TICKER','QUICK_RATIO','CURRENT_RATIO','DEBT_EQUITY_RATIO', 'DEBT_TOTAL_CAPITAL'])

df_us_companies_debt = df_us_companies_debt.rename(columns={"COMPANY_NAME": "Company Name", "TICKER": "Ticker", "QUICK_RATIO": "Quick Ratio", "CURRENT_RATIO": "Current Ratio", 
"DEBT_EQUITY_RATIO": "Debt/Equity Ratio","DEBT_TOTAL_CAPITAL": "Debt/Total Capital"
})

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_us_companies_debt, False, 0)

#########
# Value #
#########

sheet_name = 'Value'

df_us_companies_value = df_us_companies.filter(['COMPANY_NAME','TICKER','PRICE_SALES_RATIO','PRICE_BOOK_RATIO','CURRENT_ROE_TTM'])

df_us_companies_value = df_us_companies_value.rename(columns={"COMPANY_NAME": "Company Name", "TICKER": "Ticker", "PRICE_SALES_RATIO": "Price/Sales", "PRICE_BOOK_RATIO": "Price/Book", 
"CURRENT_ROE_TTM": "Current ROE"
})

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_us_companies_value, False, 0)

print("Done!")