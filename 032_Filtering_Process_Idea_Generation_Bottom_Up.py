import pandas as pd
from datetime import datetime
from common import convert_excelsheet_to_dataframe, write_dataframe_to_excel
from common import get_zacks_us_companies
from common import get_stockrow_stock_data

###############################################################
#       BEWARE: THIS SCRIPT TAKES 14 HOURS TO COMPLETE        #
###############################################################

def calculate_growth(df):

    count = 0
    for (columnName, columnData) in df.iteritems():
        count += 1
        if(count == 1):
            year_a_name = columnName
            year_a_value = columnData[0]
        else:
            year_b_name = columnName
            year_b_value = columnData[0]
            col_name = "%s_%s_GROWTH" % (year_a_name, year_b_name)
            df[col_name] = (year_b_value-year_a_value)/year_a_value

            year_a_name = year_b_name
            year_a_value = year_b_value

    return df

df_us_companies = get_zacks_us_companies()

excel_file_path = '/Trading_Excel_Files/04_Filtering_Process/030_Filtering_Process_Quantitative_Analysis_US_Stocks.xlsm'
sheet_name = 'Database US Companies'
df_us_companies = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

df_us_companies_profile = df_us_companies.filter(['COMPANY_NAME','TICKER','SECTOR','INDUSTRY','MARKET_CAP'])

#For Debug Purposes
df_us_companies_profile = df_us_companies_profile.head(2)

now_start = datetime.now()
start_time = now_start.strftime("%H:%M:%S")

df_sales_data_all_companies = pd.DataFrame()
df_eps_data_all_companies = pd.DataFrame()
df_cashflow_data_all_companies = pd.DataFrame()

count = 0
total = len(df_us_companies_profile)
for ticker in df_us_companies_profile["TICKER"]:
    count += 1
    print("%s/%s - %s" % (count, total, ticker))
    try:
        df_stockrow_data = get_stockrow_stock_data(ticker, False)
        
        df_sales_data = df_stockrow_data.filter(['SALES']).T
        df_sales_data = calculate_growth(df_sales_data)

        df_eps_data = df_stockrow_data.filter(['EARNINGS_PER_SHARE']).T
        df_eps_data = calculate_growth(df_eps_data)

        df_cashflow_data = df_stockrow_data.filter(['CASH_FLOW_PER_SHARE']).T
        df_cashflow_data = calculate_growth(df_cashflow_data)

        df_sales_data = df_sales_data.reset_index(drop=True)
        df_eps_data = df_eps_data.reset_index(drop=True)
        df_cashflow_data = df_cashflow_data.reset_index(drop=True)

        df_company_profile = df_us_companies_profile.loc[df_us_companies_profile['TICKER'] == ticker].reset_index(drop=True)

        df_sales_data = pd.concat([df_company_profile, df_sales_data], axis=1, join="inner")
        df_eps_data = pd.concat([df_company_profile, df_eps_data], axis=1, join="inner")
        df_cashflow_data = pd.concat([df_company_profile, df_cashflow_data], axis=1, join="inner")

        df_sales_data_all_companies = df_sales_data_all_companies.append(df_sales_data)
        df_eps_data_all_companies = df_eps_data_all_companies.append(df_eps_data)
        df_cashflow_data_all_companies = df_cashflow_data_all_companies.append(df_cashflow_data)
    except:
        print("Did not load data")

now_finish = datetime.now()
finish_time = now_finish.strftime("%H:%M:%S")

difference = now_finish - now_start

seconds_in_day = 24 * 60 * 60

print("Start Time = %s" % (start_time))
print("End Time = %s" % (finish_time))
print(divmod(difference.days * seconds_in_day + difference.seconds, 60))

excel_file_path = '/Trading_Excel_Files/04_Filtering_Process/032_Filtering_Process_Idea_Generation_Bottom_Up.xlsm'
sheet_name = 'Sales'
# Write the updated df to the excel sheet, and overwrite what was there before
write_dataframe_to_excel(excel_file_path, sheet_name, df_sales_data_all_companies, False, 0, True)

sheet_name = 'Earnings'
write_dataframe_to_excel(excel_file_path, sheet_name, df_eps_data_all_companies, False, 0, True)

sheet_name = 'FCF'
write_dataframe_to_excel(excel_file_path, sheet_name, df_cashflow_data_all_companies, False, 0, True)

print("Done!")
