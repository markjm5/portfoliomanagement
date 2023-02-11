import pandas as pd
from datetime import date
from common import convert_excelsheet_to_dataframe, write_dataframe_to_excel
from common import combine_df_on_index, get_yf_historical_stock_data

excel_file_path = '/Trading_Excel_Files/04_Filtering_Process/024_Filtering_Process_ETF_Universe.xlsm'

####################################
# Get Asset Class Data from YF.com #
####################################

def get_etf_data(etfs):

    # Convert the dictionary into DataFrame
    df_etf_data = pd.DataFrame(data)

    #get date range
    todays_date = date.today()
    date_str = "%s-%s-%s" % (todays_date.year, todays_date.month, todays_date.day)   

    #Get the raw data
    for etf in etfs:
        print("Getting Data For: %s" % (etf,))
        df_etf = get_yf_historical_stock_data(etf, "1d", "2007-01-01", date_str)

        #Remove unnecessary columns and rename columns
        df_etf = df_etf.drop(['Open', 'High', 'Low', 'Volume'], axis=1)
        df_etf = df_etf.rename(columns={"Close": etf})
        df_etf_data = combine_df_on_index(df_etf_data, df_etf, 'DATE')

    return df_etf_data

sheet_name = 'Database'
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True, None,'%d/%m/%Y')

data = {'DATE': []}

#get date range
todays_date = date.today()
date_str = "%s-%s-%s" % (todays_date.year, todays_date.month, todays_date.day)

#ETFs
europe_sectors_etf = [ 'EXV5.DE','EXV1.DE','EXV6.DE','EXV7.DE','EXV8.DE','EXH2.DE','EXH3.DE','EXV4.DE','EXH4.DE','EXH5.DE','EXH6.DE',
    'EXH1.DE','EXH7.DE','EXI5.DE','EXH8.DE','EXSA.DE','EXV3.DE','EXV2.DE','EXV9.DE','EXH9.DE']

market_cap_growth_value_etf = ['IWV','OEF','IJH','IJR','IWC','PFM','PEY','VUG','IJT','IWP','IJK','IVW','IWO','IWF','VTV',
    'IJS','IWS','PWV','IJJ','IVE','IWN','IWD']

americas_etf = ['ACWI','SPY','QQQ','DIA','IYT','IDU','IWM','EWC','EWW','ARGT','EWZ','GXG','ECH','EPU']

emea_etf = ['EWU','EIRL','EWG','EWQ','EWO','EWN','EDEN','EFNL','EWK','EPOL','NORW','EWI','EWD','EWL','EWP','PGAL','GREK',
'TUR','EIS','NGE','EZA','EGPT']

apac_etf = ['EWJ','FXI','EWH','ENZL','EWS','INDY','EWY','EWA','EWM','THD','IDX','EWT','VNM','EPHE']

industries_etf = ['XAR','MOO','JETS','KBE','IBB','IAI','BJK','SKYY','KOL','XLY','XLP','HACK','EEM','XLE','XLF','PBJ','GLD','GDX',
'XLV','XHE','XHS','XHB','XLI','KIE','FDN','XLB','PBS','XME','IGN','OIH','IHE','KRE','IYR','XRT','SMH','SEA','SLV','SOCL',
'XSW','TAN','SPY','SLX','XLK','IYZ','CUT','TLT','XTN','UUP','XLU','PHO']

commodities_etf = ['JJU','SLX','KOL','JO','JJC','CORN','BAL','UGA','GLD','JJG','DJP','COW','UNG','JJN','USO','PALL','SPY',
'SLV','SOYB','SGG','WEAT','WOOD']

etfs = []

#combine lists
etfs.extend(europe_sectors_etf)
etfs.extend(market_cap_growth_value_etf)
etfs.extend(americas_etf)
etfs.extend(emea_etf)
etfs.extend(apac_etf)
etfs.extend(industries_etf)
etfs.extend(commodities_etf)

#remove duplicates
etfs = list(dict.fromkeys(etfs))

df_etf_data = get_etf_data(etfs)
df_etf_data = df_etf_data.fillna(method='ffill')

df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

df_updated = combine_df_on_index(df_original, df_etf_data, 'DATE')

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

print("Done!")
