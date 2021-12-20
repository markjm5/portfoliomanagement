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
  current_month = None
  current_year = None

  for i in range(len(json["refRates"])):
    datetime_ref_rates = datetime.strptime(json["refRates"][i]['effectiveDate'], '%Y-%m-%d')

    if('%s-%s' % (datetime_ref_rates.year,datetime_ref_rates.month) != '%s-%s' % (current_year,current_month)):
      current_month = datetime_ref_rates.month
      current_year = datetime_ref_rates.year
      converted_date = datetime.strptime('%s-%s-%s' % (current_year, current_month, '01'), '%Y-%m-%d')
      try:
          df = df.append({"DATE": converted_date, dimensions[0].upper(): str(json["refRates"][i]['targetRateTo'])}, ignore_index=True)
      except KeyError as e:
          df = df.append({"DATE": converted_date, dimensions[0].upper(): str(json["refRates"][i]['targetRateFrom'])}, ignore_index=True)

  df['DATE'] = pd.to_datetime(df['DATE'],format='%Y-%m-%d')
  df[dimensions[0].upper()] = pd.to_numeric(df[dimensions[0].upper()])

  df = df.sort_values(by='DATE')
  df = df.reset_index(drop=True)

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

currentMonth = datetime.now().month
currentYear = datetime.now().year

startDate = '01/01/1971'
endDate = '%s/01/%s' % (currentMonth, currentYear)

df_ff_target_rate = get_newyorkfed_target_rate('unsecured', [dataset,startDate,endDate])

df_FEDFUNDS = df_FEDFUNDS.merge(df_ff_target_rate, how="left")

#Reorder columns
# get a list of columns
cols = list(df_FEDFUNDS)

# Move the column to head of list using index, pop and insert
cols.insert(0, cols.pop(cols.index('DATE')))
cols.insert(1, cols.pop(cols.index('EFFR')))
cols.insert(2, cols.pop(cols.index('FEDFUNDS')))

# Reorder
df_FEDFUNDS = df_FEDFUNDS[cols]

# Rename columns
df_FEDFUNDS = df_FEDFUNDS.rename(columns={'EFFR': 'Fed Target', 'FEDFUNDS': 'Fed Funds Rate'})

write_dataframe_to_excel(excel_file_path, sheet_name, df_FEDFUNDS, False, 0)

print("Done!")