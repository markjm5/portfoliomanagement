import requests
import os.path
import csv
import pandas as pd
from common import get_stlouisfed_data, convert_excelsheet_to_dataframe, write_dataframe_to_excel, combine_df, write_to_directory
from datetime import datetime

excel_file_path = '/trading_excel_files/01_lagging_coincident_indicators/007_Lagging_Indicator_US_Inflation.xlsm'

def get_newyorkfed_target_rate(type, dimensions):

  #Use Dataset, Dimensions to construct the below URL and make GET request
  #https://markets.newyorkfed.org/api/rates/unsecured/effr/search.json?startDate=01/01/1971&endDate=01/11/2021

  url = "https://markets.newyorkfed.org/api/rates/%s/%s/search.json?startDate=%s&endDate=%s" % (type,dimensions[0], dimensions[1], dimensions[2])

  resp = requests.get(url=url)
  json = resp.json() 

  df = pd.DataFrame(columns=["DATE",dimensions[0].upper()])

  for i in range(len(json["refRates"])):
    try:
        df = df.append({"DATE": json["refRates"][i]['effectiveDate'], dimensions[0].upper(): str(json["refRates"][i]['targetRateTo'])}, ignore_index=True)
    except KeyError as e:
        df = df.append({"DATE": json["refRates"][i]['effectiveDate'], dimensions[0].upper(): str(json["refRates"][i]['targetRateFrom'])}, ignore_index=True)

  df = df.sort_values(by='DATE')

  return df  

##################################
#   Get Data from St Louis Fed   #
##################################

sheet_name = 'Database'

df_CPIAUCSL = get_stlouisfed_data('CPIAUCSL')
df_CPIENGSL = get_stlouisfed_data('CPIENGSL')
df_CPIFABSL = get_stlouisfed_data('CPIFABSL')
df_CPILFESL = get_stlouisfed_data('CPILFESL')

#Combine all these data frames into a single data frame based on the DATE field
df = pd.merge(df_CPIAUCSL,df_CPIENGSL,"left")
df = pd.merge(df,df_CPIFABSL,"left")
df = pd.merge(df,df_CPILFESL,"left")

df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

df_updated = combine_df(df_original, df)

write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

###################################################
#   Get Data from St Louis Fed and New York Fed   #
###################################################

sheet_name = 'Fed Fund New'

df_FEDFUNDS = get_stlouisfed_data('FEDFUNDS')
#Get Fed Funds Target from markets.newyorkfed.org
#unsecured/effr/search.json?startDate=01/01/1971&endDate=11/01/2021
dataset = 'effr'
startDate = '01/01/1971'
endDate = '11/01/2021'
df_ff_target_rate = get_newyorkfed_target_rate('unsecured', [dataset,startDate,endDate])

df_ff_target_rate['DATE'] = pd.to_datetime(df_ff_target_rate['DATE'],format='%Y-%m-%d')
df_ff_target_rate['EFFR'] = pd.to_numeric(df_ff_target_rate['EFFR'])

#TODO: Fill in blanks in EFFR

df_FEDFUNDS = df_FEDFUNDS.merge(df_ff_target_rate, how="left")

#TODO: Rename and reorder columns


write_dataframe_to_excel(excel_file_path, sheet_name, df_FEDFUNDS, False, 0)

#Write to a csv file in the correct directory
#write_to_directory(df,'007_Lagging_Indicator_US_Inflation.csv')
#write_to_directory(df_FEDFUNDS,'007_Lagging_Indicator_US_Fed_Fund_Rate_Target.csv')

print("Done!")