import pandas as pd
from datetime import datetime as dt
from datetime import date
from bs4 import BeautifulSoup
from requests.models import parse_header_links
from common import get_oecd_data, convert_excelsheet_to_dataframe, write_dataframe_to_excel
from common import combine_df_on_index, get_stlouisfed_data, get_page

excel_file_path = '/Trading_Excel_Files/02_Interest_Rates_FX/014_FX_Commitment_of_Traders.xlsm'
sheet_name = 'Database'

df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

def return_df(df, value,ticker):
    df = df.loc[df['CONTRACT_UNITS'].isin([value])].reset_index(drop=True)
    df = df.drop(['NON_COMMERCIAL_POSITIONS_LONG', 'NON_COMMERCIAL_POSITIONS_SHORT', 'CONTRACT_UNITS'], axis=1)
    df = df.rename(columns={"NON_COMMERCIAL_POSITIONS_NET": ticker})

    return df


data_sheet_raw = 'Futures Only Report Raw'
df_data_raw = convert_excelsheet_to_dataframe(excel_file_path, data_sheet_raw, False)

#TODO: Clean up raw data and combine with df_original
df_data_raw['As_of_Date_In_Form_YYMMDD'] = pd.to_datetime(df_data_raw['As_of_Date_In_Form_YYMMDD'],format='%y%m%d')

df_data_raw = df_data_raw[["As_of_Date_In_Form_YYMMDD", "NonComm_Positions_Long_All","NonComm_Positions_Short_All","Contract_Units"]]

df_data_raw = df_data_raw.rename(columns={"As_of_Date_In_Form_YYMMDD": "DATE", "NonComm_Positions_Long_All": "NON_COMMERCIAL_POSITIONS_LONG", "NonComm_Positions_Short_All": "NON_COMMERCIAL_POSITIONS_SHORT", "Contract_Units": "CONTRACT_UNITS"})
df_data_raw = df_data_raw.reset_index(drop=True)
df_data_raw['NON_COMMERCIAL_POSITIONS_NET'] = df_data_raw['NON_COMMERCIAL_POSITIONS_LONG'] - df_data_raw['NON_COMMERCIAL_POSITIONS_SHORT']

#TODO: Write updated df_original back to Database sheet
#df_cols = df_updated.groupby(['CONTRACT_UNITS']).count()

df_data_aud = return_df(df_data_raw,"(CONTRACTS OF AUD 100,000)","AUD")
df_data_brl = return_df(df_data_raw,"(CONTRACTS OF BRL 100,000)","BRL")
df_data_cad = return_df(df_data_raw,"(CONTRACTS OF CAD 100,000)","CAD")
df_data_chf = return_df(df_data_raw,"(CONTRACTS OF CHF 125,000)","CHF")
df_data_eur = return_df(df_data_raw,"(CONTRACTS OF EUR 125,000)","EUR")
df_data_gbp = return_df(df_data_raw,"(CONTRACTS OF GBP 62,500)","GBP")
df_data_jpy = return_df(df_data_raw,"(CONTRACTS OF JPY 12,500,000)","JPY")
df_data_mxn = return_df(df_data_raw,"(CONTRACTS OF MXN 500,000)","MEX")
df_data_nzd = return_df(df_data_raw,"(CONTRACTS OF NZD 100,000)","NZD")
df_data_rub = return_df(df_data_raw,"(CONTRACTS OF RUB 2,500,000)","RUB")
df_data_zar = return_df(df_data_raw,"(CONTRACTS OF ZAR 500,000)","ZAR")

df_new = combine_df_on_index(df_data_eur, df_data_cad, 'DATE')
df_new = combine_df_on_index(df_new, df_data_mxn, 'DATE')
df_new = combine_df_on_index(df_new, df_data_jpy, 'DATE')
df_new = combine_df_on_index(df_new, df_data_nzd, 'DATE')
df_new = combine_df_on_index(df_new, df_data_zar, 'DATE')
df_new = combine_df_on_index(df_new, df_data_aud, 'DATE')
df_new = combine_df_on_index(df_new, df_data_chf, 'DATE')
df_new = combine_df_on_index(df_new, df_data_gbp, 'DATE')
df_new = combine_df_on_index(df_new, df_data_rub, 'DATE')
df_new = combine_df_on_index(df_new, df_data_brl, 'DATE')

#TODO: Fix issue with doubling of rows

import pdb; pdb.set_trace()

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

print("Done!")
