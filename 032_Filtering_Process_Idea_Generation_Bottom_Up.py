from unittest import skip
import pandas as pd
import re
from datetime import date, datetime
from common import get_stlouisfed_data, convert_excelsheet_to_dataframe, write_dataframe_to_excel
from common import combine_df_on_index, get_page, get_page_selenium, convert_html_table_to_df, get_zacks_us_companies
from bs4 import BeautifulSoup

from common import get_finwiz_stock_data, get_stockrow_stock_data, get_zacks_balance_sheet_shares
from common import get_zacks_peer_comparison, get_zacks_earnings_surprises, get_zacks_product_line_geography

df_us_companies = get_zacks_us_companies()

excel_file_path = '/Trading_Excel_Files/04_Filtering_Process/030_Filtering_Process_Quantitative_Analysis_US_Stocks.xlsm'
sheet_name = 'Database US Companies'
df_us_companies = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

df_us_companies_fcf = df_us_companies.filter(['COMPANY_NAME','TICKER','SECTOR','INDUSTRY','MARKET_CAP'])

#from common import get_finwiz_stock_data, get_stockrow_stock_data, get_zacks_balance_sheet_shares
#from common import get_zacks_peer_comparison, get_zacks_earnings_surprises, get_zacks_product_line_geography
now_start = datetime.now()
start_time = now_start.strftime("%H:%M:%S")

count = 0
total = len(df_us_companies)
for ticker in df_us_companies["TICKER"]:
    count += 1
    print("%s / %s - %s" % (count, total, ticker))
    try:
        stockrow_data = get_stockrow_stock_data(ticker, False)
        
        stockrow_data = stockrow_data.filter(['SALES','EARNINGS_PER_SHARE'])

        #TODO: Create 2 dfs for all these tickers - SALES and EARNINGS_PER_SHARE
        #TODO: Write completed dfs into separate sheets
    except:
        print("Did not load data")

    import pdb; pdb.set_trace()

now_finish = datetime.now()
finish_time = now_finish.strftime("%H:%M:%S")

difference = now_finish - now_start

seconds_in_day = 24 * 60 * 60

print("Start Time = %s" % (start_time))
print("End Time = %s" % (finish_time))
print(divmod(difference.days * seconds_in_day + difference.seconds, 60))

# FCF 2015	FCF 2016	FCF 2017	FCF 2018
# EPS 2015	EPS 2016	EPS 2017	EPS 2018
# Sales 2018	Sales 2019	Sales 2020
# ROE
# AAll

excel_file_path = '/Trading_Excel_Files/04_Filtering_Process/031_Filtering_Process_Idea_Generation_Bottom_Up.xlsm'
sheet_name = 'Database US Companies'

print("Done!")
