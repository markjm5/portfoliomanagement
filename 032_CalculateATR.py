# Import the pandas library
import pandas as pd
from datetime import date
from common import convert_excelsheet_to_dataframe, write_dataframe_to_excel, get_yf_historical_stock_data, combine_df_on_index

excel_file_path = '/Trading_Excel_Files/04_Filtering_Process/032_TickerATR.xlsm'

def return_atr(df_data):
    # Creates a new column in the netflix dataframe called 'H-L' and does the high - low
    df_data['H-L'] = df_data['High'] - df_data['Low']

    # Creates a new column in the netflix dataframe called 'H-C' which is the absolute value of the high on the current day - close previous day
    # the .shift(1) function takes the close from the previous day
    df_data['H-C'] = abs(df_data['High'] - df_data['Close'].shift(1))

    # Creates a new column in the netflix dataframe called 'L-C' which is the absolute value of the low on the current day - close previous day
    df_data['L-C'] = abs(df_data['Low'] - df_data['Close'].shift(1))

    # Creates a new column in the netflix dataframe called 'TR' which chooses which is the highest out of the H-L, H-C and L-C values
    df_data['TR'] = df_data[['H-L', 'H-C', 'L-C']].max(axis=1)

    # Creates a new column in the netflix datafram called 'ATR' and calculates the ATR
    df_data['ATR'] = df_data['TR'].rolling(14).mean()/100

    #Remove unnecessary columns from df_EUR_USD and rename columns
    df_data = df_data.drop(['Open', 'High', 'Low', 'Volume'], axis=1)

    # Creates a new dataframe called netflix_sorted_df using the netflix dataframe
    # Sorts the dates from newest to oldest, rather than oldest to newest which the Yahoo Finance default
    df_sorted = df_data.sort_values(by='DATE', ascending = False)

    return df_sorted

###############################
# Get Ticker Data from YF.com #
###############################

sheet_name = 'Ticker'
df_ticker = convert_excelsheet_to_dataframe(excel_file_path, sheet_name)

index = df_ticker['Ticker'].values[0]

#get date range
todays_date = date.today()
date_str = "%s-%s-%s" % (todays_date.year, todays_date.month, todays_date.day)

print("Getting: %s" % index)

#################
# Get Daily ATR #
#################

df_data = get_yf_historical_stock_data(index, "1d", "2000-12-28", date_str)

df_sorted_daily = return_atr(df_data)

sheet_name = 'Daily ATR'
df_original_indexes = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True, None,'%d/%m/%Y')

df_updated_indexes = combine_df_on_index(df_original_indexes, df_sorted_daily, 'DATE')

write_dataframe_to_excel(excel_file_path, sheet_name, df_updated_indexes, False, 0)

###################
# Get Monthly ATR #
###################

df_data = get_yf_historical_stock_data(index, "1mo", "2000-12-28", date_str)

df_sorted_monthly = return_atr(df_data)

sheet_name = 'Monthly ATR'
df_original_indexes = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True, None,'%d/%m/%Y')

df_updated_indexes = combine_df_on_index(df_original_indexes, df_sorted_monthly, 'DATE')

write_dataframe_to_excel(excel_file_path, sheet_name, df_updated_indexes, False, 0)

#####################
# Get Quarterly ATR #
#####################

df_data = get_yf_historical_stock_data(index, "3mo", "2000-12-28", date_str)

df_sorted_quarterly = return_atr(df_data)

sheet_name = 'Quarterly ATR'
df_original_indexes = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True, None,'%d/%m/%Y')

df_updated_indexes = combine_df_on_index(df_original_indexes, df_sorted_quarterly, 'DATE')

write_dataframe_to_excel(excel_file_path, sheet_name, df_updated_indexes, False, 0)

print("Done!")