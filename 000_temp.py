import re
import os
import pandas as pd
from bs4 import BeautifulSoup
from common import get_data_fred, get_sp500_monthly_prices, convert_excelsheet_to_dataframe, write_dataframe_to_excel
from common import combine_df_on_index, get_ism_manufacturing_content, scrape_ism_manufacturing_headline_index, _util_check_diff_list

log_file_path = '/Trading_Excel_Files/error_log.txt'

filepath = os.getcwd()
log_file_path = filepath + log_file_path #.replace("/","\\")


arr_tickers = []
pattern = re.compile(r'finviz[.]com[/]quote.ashx[?]t[=][A-Z]*')

with open(log_file_path) as f:
    lines = f.readlines()

    for line in lines:
        try:
            matches_comment = re.search(pattern,line).group(0)
            ticker = matches_comment.replace('finviz.com/quote.ashx?t=','')
            if ticker not in arr_tickers:
                arr_tickers.append(ticker)
        except Exception as e:
            pass

print(arr_tickers)
import pdb; pdb.set_trace()                          

print('Done!')