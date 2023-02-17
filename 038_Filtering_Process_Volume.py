from datetime import datetime
import pandas as pd
import numpy as np
import yfinance as yf
from common import convert_excelsheet_to_dataframe, write_dataframe_to_excel
from common import get_yf_key_stats

###############################################################
#       BEWARE: THIS SCRIPT TAKES 2H 30MIN TO COMPLETE        #
###############################################################

def transpose_df_string_numbers(df, column):
    df[column] = df[column].str.replace("M","")
    df[column] = df[column].str.replace("k","")
    df[column] = df[column].str.replace("N/A","0.00")
    #Convert numbers to numeric
    df[column] = df[column].astype(float)
    #where bool_million=True, multiply by a million, otherwise, multiply by a thousand
    df[column]  = np.where(df['bool_million'] == True, df[column]*1000000, df[column]*1000)
    df[column] = df[column].astype(int)

    return df

excel_file_path = '/Trading_Excel_Files/04_Filtering_Process/030_Filtering_Process_Quantitative_Analysis_US_Stocks.xlsm'
sheet_name = 'Database US Companies'
df_us_companies = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

df_us_companies_profile = df_us_companies.filter(['COMPANY_NAME','TICKER','SECTOR','INDUSTRY','MARKET_CAP','SHARES_OUTSTANDING_MILLIONS'])

#For Debug Purposes
#df_us_companies_profile = df_us_companies_profile.head(2)

now_start = datetime.now()  
start_time = now_start.strftime("%H:%M:%S")

df_vol_data_all_companies = pd.DataFrame()

count = 0
total = len(df_us_companies_profile)
for ticker in df_us_companies_profile["TICKER"]:
    count += 1
    print("%s/%s - %s" % (count, total, ticker))
    try:
        df_yf_key_statistics = get_yf_key_stats(ticker)

        df_yf_key_statistics = df_yf_key_statistics.filter(['AVG_VOL_10D','AVG_VOL_3M']).reset_index(drop=True)
        df_yf_stock_data_last = yf.download(tickers=ticker,period="1d",interval="1d",auto_adjust=True)
        df_daily_volume = df_yf_stock_data_last["Volume"]
        df_daily_volume = df_daily_volume.to_frame().reset_index(drop=True)

        df_company_profile = df_us_companies_profile.loc[df_us_companies_profile['TICKER'] == ticker].reset_index(drop=True)

        df_volume_data = pd.concat([df_company_profile, df_daily_volume], axis=1, join="inner")
        df_volume_data = pd.concat([df_volume_data, df_yf_key_statistics], axis=1, join="inner")

        df_vol_data_all_companies = df_vol_data_all_companies.append(df_volume_data)

        #TODO: Add total shares outstanding, so we can calculate it as a % of volume traded

    except:
        print("Did not load data")
now_finish = datetime.now()
finish_time = now_finish.strftime("%H:%M:%S")

difference = now_finish - now_start

seconds_in_day = 24 * 60 * 60

print("Start Time = %s" % (start_time))
print("End Time = %s" % (finish_time))
print(divmod(difference.days * seconds_in_day + difference.seconds, 60))

df_vol_data_all_companies = df_vol_data_all_companies.reset_index(drop=True)

#Create temp column to track millions and thousands
df_vol_data_all_companies['bool_million'] = np.where(df_vol_data_all_companies['AVG_VOL_10D'].str.contains("M"), True, False)
df_vol_data_all_companies = transpose_df_string_numbers(df_vol_data_all_companies,'AVG_VOL_10D')

#Updated temp column to track millions and thousands
df_vol_data_all_companies['bool_million'] = np.where(df_vol_data_all_companies['AVG_VOL_3M'].str.contains("M"), True, False)
df_vol_data_all_companies = transpose_df_string_numbers(df_vol_data_all_companies,'AVG_VOL_3M')

#Drop temp column
df_vol_data_all_companies = df_vol_data_all_companies.drop(['bool_million'], axis=1)

#Create calculated metrics
df_vol_data_all_companies['VS_10_DAYS'] = df_vol_data_all_companies['Volume']/df_vol_data_all_companies['AVG_VOL_10D']
df_vol_data_all_companies['VS_3_MONTHS'] = df_vol_data_all_companies['Volume']/df_vol_data_all_companies['AVG_VOL_3M']

df_vol_data_all_companies['SHARES_OUTSTANDING_MILLIONS'] = df_vol_data_all_companies['SHARES_OUTSTANDING_MILLIONS']*1000000
#df_vol_data_all_companies['SHARES_OUTSTANDING_MILLIONS'] = df_vol_data_all_companies['SHARES_OUTSTANDING_MILLIONS'].astype(int)
df_vol_data_all_companies['DAILY_SHARES_TRADED_PERCENTAGE'] = df_vol_data_all_companies['Volume']/df_vol_data_all_companies['SHARES_OUTSTANDING_MILLIONS']

excel_file_path = '/Trading_Excel_Files/04_Filtering_Process/038_Filtering_Process_Volume.xlsm'
sheet_name = 'Volume'

write_dataframe_to_excel(excel_file_path, sheet_name, df_vol_data_all_companies, False, 0, True)

print("Done!")
