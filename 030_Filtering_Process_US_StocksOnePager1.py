from json.encoder import py_encode_basestring
from numpy import subtract
import pandas as pd
import json
from datetime import date
from datetime import datetime as dt
from common import convert_excelsheet_to_dataframe
from common import get_page, get_finwiz_stock_data, get_stockrow_stock_data, get_zacks_balance_sheet_shares
from common import get_zacks_peer_comparison, get_zacks_earnings_surprises, get_zacks_product_line_geography
from common import write_value_to_cell_excel, check_sheet_exists, create_sheet
from common import download_file, unzip_file, get_yf_key_stats,transpose_df, get_zacks_us_companies

debug = False

#Dates
todays_date = date.today()
one_year_ago = dt(todays_date.year - 1, 12, 31)
two_year_ago = dt(todays_date.year - 2, 12, 31)
three_year_ago = dt(todays_date.year - 3, 12, 31)
list_dates = []
list_dates.append(one_year_ago)
list_dates.append(two_year_ago)
list_dates.append(three_year_ago)


def get_data(ticker):

    #TODO: Get peer comparison from here instead?: https://www.marketwatch.com/investing/stock/aapl
    df_zacks_balance_sheet_shares_annual, df_zacks_balance_sheet_shares_quarterly = get_zacks_balance_sheet_shares(ticker)

    df_zacks_peer_comparison = get_zacks_peer_comparison(ticker)

    df_zacks_next_earnings_release, df_zacks_earnings_surprises = get_zacks_earnings_surprises(ticker)
    df_zacks_product_line, df_zacks_geography = get_zacks_product_line_geography(ticker)
    df_finwiz_stock_data = get_finwiz_stock_data(ticker)
    df_stockrow_data = get_stockrow_stock_data(ticker, debug)

    url_yf_modules = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/%s?modules=summaryProfile,financialData,summaryDetail,price,defaultKeyStatistics" % (ticker)
    json_yf_modules = json.loads(get_page(url_yf_modules).content)

    df_yf_key_statistics = get_yf_key_stats(ticker)

    df_peer_metrics = pd.DataFrame(columns=['TICKER','MARKET_CAP','EV','PE','EV_EBITDA','EV_EBIT','EV_REVENUE','PB','EBITDA_MARGIN','EBIT_MARGIN','NET_MARGIN','DIVIDEND_YIELD','ROE'])
    peer_ticker_list = []
    #Retrieve company peers metrics
    #TODO: Fix percentage metrics that do not make sense
    for row,peer in df_zacks_peer_comparison.iterrows():
        temp_row = []
        peer_ticker = peer[1]
        df_peer_zacks_stock_data = df_us_companies.loc[df_us_companies['TICKER'] == peer_ticker].reset_index(drop=True)
        if(len(df_peer_zacks_stock_data) > 0):
            peer_ticker_list.append(peer_ticker)
            peer_market_cap = df_peer_zacks_stock_data['MARKET_CAP'].values[0]

            #Calculate EV
            peer_current_assets = df_peer_zacks_stock_data['CURRENT_ASSETS(MILLION)'].values[0]
            peer_current_liabilities = df_peer_zacks_stock_data['CURRENT_LIABILITIES(MILLION)'].values[0]
            peer_long_term_debt = df_peer_zacks_stock_data['LONG_TERM_DEBT(MILLION)'].values[0]

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

            try:
                peer_ebitda_margin = round(df_peer_zacks_stock_data['EBITDA_MIL'].values[0]/df_peer_zacks_stock_data['ANNUAL_SALES(MILLION)'].values[0],2) # EBITDA margin - Can be calculated using EBITDA?
            except ArithmeticError:
                peer_ebitda_margin = 0

            try:
                peer_ebit_margin = round(df_peer_zacks_stock_data['EBIT_MIL'].values[0]/df_peer_zacks_stock_data['ANNUAL_SALES(MILLION)'].values[0],2) # EBITDA margin - Can be calculated using EBITDA?
            except ArithmeticError:
                peer_ebit_margin = 0

            try:
                peer_net_margin = df_peer_zacks_stock_data['NET_MARGIN_PERCENTAGE'].values[0]/100
            except ArithmeticError:
                peer_net_margin = 0

            try:
                peer_dividend_yield = df_peer_zacks_stock_data['DIVIDEND_YIELD_PERCENTAGE'].values[0]/100
            except ArithmeticError:
                peer_dividend_yield = 0

            try:
                peer_roe = df_peer_zacks_stock_data['CURRENT_ROE_TTM'].values[0]/100
            except ArithmeticError:
                peer_dividend_yield = 0

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
        if(len(temp_row) == len(df_peer_metrics.columns)):
            df_peer_metrics.loc[len(df_peer_metrics.index)] = temp_row   

    df_peer_metrics = transpose_df(df_peer_metrics)

    df_peer_metrics = df_peer_metrics.reset_index()
    df_peer_metrics = df_peer_metrics.rename(columns={"index": "METRIC"}) 
    df_peer_metrics = df_peer_metrics.rename_axis(None, axis=1)

    return df_zacks_next_earnings_release, df_zacks_earnings_surprises, df_zacks_product_line, df_zacks_geography, df_finwiz_stock_data, df_stockrow_data, json_yf_modules, df_yf_key_statistics, df_peer_metrics, df_zacks_balance_sheet_shares_annual, df_zacks_balance_sheet_shares_quarterly


#Get company data from various sources
df_us_companies = get_zacks_us_companies()

#ticker = "CRM"

for ticker in df_us_companies["TICKER"]:
    print(ticker)
    df_zacks_next_earnings_release, df_zacks_earnings_surprises, df_zacks_product_line, df_zacks_geography, df_finwiz_stock_data, df_stockrow_data, json_yf_modules, df_yf_key_statistics, df_peer_metrics, df_zacks_balance_sheet_shares_annual, df_zacks_balance_sheet_shares_quarterly = get_data(ticker)

import pdb; pdb.set_trace()
