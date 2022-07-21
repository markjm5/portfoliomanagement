import pandas as pd
from common import convert_excelsheet_to_dataframe, write_dataframe_to_excel
from common import combine_df_on_index

excel_file_path = '/Trading_Excel_Files/02_Interest_Rates_FX/014_FX_Commitment_of_Traders.xlsm'
sheet_name = 'Database'

df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

def return_df(df, search_str,ticker):
    print("Getting Data For: %s" % (ticker))
    df = df[df.CONTRACT_UNITS.str.contains(search_str,case=False)].groupby(['DATE']).sum().reset_index(drop=False)
    #df = df.drop(['NON_COMMERCIAL_POSITIONS_LONG', 'NON_COMMERCIAL_POSITIONS_SHORT', 'CONTRACT_UNITS'], axis=1)
    df = df.drop(['NON_COMMERCIAL_POSITIONS_LONG', 'NON_COMMERCIAL_POSITIONS_SHORT'], axis=1)
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

df_data_aud = return_df(df_data_raw,"CONTRACTS OF AUD","AUD")
df_data_brl = return_df(df_data_raw,"CONTRACTS OF BRL","BRL")
df_data_cad = return_df(df_data_raw,"CONTRACTS OF CAD","CAD")
df_data_chf = return_df(df_data_raw,"CONTRACTS OF CHF","CHF")
df_data_eur = return_df(df_data_raw,"CONTRACTS OF EUR","EUR")
df_data_gbp = return_df(df_data_raw,"CONTRACTS OF GBP","GBP")
df_data_jpy = return_df(df_data_raw,"CONTRACTS OF JPY","JPY")
df_data_mxn = return_df(df_data_raw,"CONTRACTS OF MXN","MEX")
df_data_nzd = return_df(df_data_raw,"CONTRACTS OF NZD","NZD")
df_data_rub = return_df(df_data_raw,"CONTRACTS OF RUB","RUB")
df_data_zar = return_df(df_data_raw,"CONTRACTS OF ZAR","ZAR")

#pd.set_option('display.max_rows', None)
#df_data_raw.groupby(['CONTRACT_UNITS']).count()
#df_data_raw.loc[df_data_raw['CONTRACT_UNITS'].isin(["(CONTRACTS OF EUR 125,000)"])].reset_index(drop=True)

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

df_updated = combine_df_on_index(df_original, df_new, 'DATE')

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

print("Done!")
