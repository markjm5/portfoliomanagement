import requests
import re
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from common import convert_excelsheet_to_dataframe, write_dataframe_to_excel
from common import combine_df_on_index, get_stlouisfed_data, get_data_fred

excel_file_path = '/Trading_Excel_Files/03_Leading_Indicators/022_Global_Macro_Data_Summary_Page.xlsm'

def get_current_ffr_target():

    url = "https://www.bankrate.com/rates/interest-rates/federal-funds-rate.aspx"

    page = requests.get(url=url)
    soup = BeautifulSoup(page.content, 'html.parser')
    
    table = soup.find('table')

    ffr_str = table.find('tbody').find('tr').find('td').text.strip()

    pattern_select = re.compile(r'([0-9\.\-]*(?=\)))')

    arr_rate = pattern_select.findall(ffr_str)
    current_ffr_target = ""

    if(len(arr_rate) > 0):
        current_ffr_target = arr_rate[0]

    # put value into a df with COL0, LAST headers
    current_ffr_target = "%s%%" % (current_ffr_target)
    data = [['Fed Funds Target Rate', current_ffr_target]]
    
    # Create the pandas DataFrame
    df_current_ffr_target = pd.DataFrame(columns=['COL0', 'LAST'], data=data)

    return df_current_ffr_target

def get_eurodollar_futures():

    url = "https://www.cmegroup.com/markets/interest-rates/stirs/eurodollar.quotes.html"
    #url = "https://www.espncricinfo.com/"

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    #chrome_options.add_argument('ignore-certificate-errors')

    #ua = UserAgent(verify_ssl=False)

    #userAgent = ua.random
    #print(userAgent)
    #import pdb; pdb.set_trace()

    #chrome_options.add_argument(f'user-agent={userAgent}')
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 640 XL LTE) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Mobile Safari/537.36 Edge/12.10166")

    driver = webdriver.Chrome(ChromeDriverManager().install(),options=chrome_options)
    driver.get(url)
    html = driver.page_source
    driver.close()

    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table')

    # Iterate through table elements and get eurodollar futures quotes
    table_rows = table.find_all('tr')
    table_rows_header = table.find_all('tr')[0].find_all('th')

    df = pd.DataFrame()
    index = 0

    for header in table_rows_header:
        df.insert(index,header.text,[],True)
        index+=1

    #Insert New Row. Format the data to show percentage as float
    for tr in table_rows:
        temp_row = []

        td = tr.find_all('td')
        if(td):
            for obs in td:
                text = obs.text
                temp_row.append(text)        

            df.loc[len(df.index)] = temp_row

    df = df.drop(columns=['Change','Options','Chart','Open', 'High', 'Low', 'Volume', 'Updated', 'PriorSettle'], axis=1)
    pattern_select = re.compile(r'((?!=[0-9])GE[A-Z][0-9])')

    df['Month'] = df['Month'].str.replace(pattern_select,'')

    # format date
    df['Month'] = pd.to_datetime(df['Month'],format='%b %Y')

    current_value = None
    one_month_value = None
    six_month_value = None
    twelve_month_value = None

    # Calculate Eurodollar Futures quotes for 1m, 6m, 12m
    for index, row in df.iterrows():
        if(index==0):
            try:
                current_value = float(row['Last'])
            except ValueError as e:
                pass        
            except TypeError as e:
                pass
        elif(index==2):
            try:
                one_month_value = current_value - float(row['Last'])
            except ValueError as e:
                pass        
            except TypeError as e:
                pass

        elif(index==5):
            try:
                six_month_value = current_value - float(row['Last'])
            except ValueError as e:
                pass        
            except TypeError as e:
                pass

        elif(index==11):
            try:
                twelve_month_value = current_value - float(row['Last'])
            except ValueError as e:
                pass        
            except TypeError as e:
                pass

        print("%s-%s" % (row['Month'], row['Last']))

    # initialize list of lists
    data = [['Euro Futures 1m', one_month_value], ['Euro Futures 6m', six_month_value], ['Euro Futures 12m', twelve_month_value]]
    
    # Create the pandas DataFrame
    df_eurodollar_futures = pd.DataFrame(columns=['COL0', 'LAST'], data=data)

    return df_eurodollar_futures    

def get_data(df):
    # retrieve Last, 6m and 12m values for data specified in df
    df_last = get_df_row(df, df['DATE'].max(), 'LAST')
    df_6_months_ago = get_df_row(df, df['DATE'].max() - pd.DateOffset(months=6), '6M')
    df_12_months_ago = get_df_row(df, df['DATE'].max() - pd.DateOffset(months=12),'12M')

    df_new = combine_df_on_index(df_last, df_6_months_ago, 'COL0')
    df_new = combine_df_on_index(df_new, df_12_months_ago, 'COL0')
    df_new = reorder_cols(df_new)
    return df_new

def get_df_row(df, date, col_name):

    # match on year and month only and don't worry about the day
    df = df.loc[(df['DATE'].dt.to_period('M') == date.to_period('M'))].T    
    df = df.iloc[1: , :]    
    df.rename(columns={ df.columns[0]: col_name }, inplace = True)
    df.reset_index(inplace=True)
    df = df.rename(columns = {'index':'COL0'})
    #in calse df has extra columns, make sure only to return COL0 and col_name fields each time
    return df.filter(['COL0',col_name])

def reorder_cols(df):
    cols = list(df)
    # move the column to head of list
    cols.insert(0, cols.pop(cols.index('COL0')))
    cols.insert(1, cols.pop(cols.index('LAST')))
    cols.insert(2, cols.pop(cols.index('6M')))
    cols.insert(3, cols.pop(cols.index('12M')))
    # reorder
    df = df[cols]
    return df

############################################
# Get US Lagging and Coincident Indicators #
############################################

#sheet_name = 'DB US Lagging Indicators'

# Get last US GDP Number (QoQ, YoY). Then get GDP numbers for 6m and 12m ago from last
df_GDPC1 = get_data_fred('GDPC1', 'GDP', 'Q')
df_us_gdp = get_data(df_GDPC1)

#US  Core CPI
df_CPILFESL = get_data_fred('CPILFESL', 'CORE_CPI', 'M')
df_core_cpi = get_data(df_CPILFESL)

#US  Core PCE
df_PCEPILFE = get_data_fred('PCEPILFE', 'CORE_PCE', 'M')
df_core_pce = get_data(df_PCEPILFE)

#US Retail Sales
df_RSAFS = get_data_fred('RSAFS','RETAIL_SALES','M')
df_retail_sales = get_data(df_RSAFS)

#US Core Retail Sales (Ex Auto and Gas)
df_MARTSSM44W72USS = get_data_fred('MARTSSM44W72USS','CORE_RETAIL_SALES','M')
df_core_retail_sales = get_data(df_MARTSSM44W72USS)

#US Unemployment Rate
df_UNRATE = get_data_fred('UNRATE', 'UNEMPLOYMENT_RATE','M')
df_unemployment_rate = get_data(df_UNRATE)

#US NFP
df_PAYEMS = get_stlouisfed_data('PAYEMS')
df_nfp = get_data(df_PAYEMS)

#US Weekly Claims
df_ICSA = get_stlouisfed_data('ICSA')
df_weekly_claims = get_data(df_ICSA)

#US Industrial Production
df_INDPRO = get_data_fred('INDPRO', 'INDUSTRIAL_PRODUCTION','M')
df_industrial_production = get_data(df_INDPRO)

# Temp df to combine all rows
df_temp = df_us_gdp.append(df_core_cpi, ignore_index=True)
df_temp = df_temp.append(df_core_pce, ignore_index=True)
df_temp = df_temp.append(df_retail_sales, ignore_index=True)
df_temp = df_temp.append(df_core_retail_sales, ignore_index=True)
df_temp = df_temp.append(df_unemployment_rate, ignore_index=True)
df_temp = df_temp.append(df_nfp, ignore_index=True)
df_temp = df_temp.append(df_weekly_claims, ignore_index=True)
df_temp = df_temp.append(df_industrial_production, ignore_index=True)

#Select Final Columns
df_lagging_indicators = df_temp.loc[df_temp['COL0'] == 'GDP_QoQ']
df_lagging_indicators = df_lagging_indicators.append(df_temp.loc[df_temp['COL0'] == 'GDP_YoY'],True)
df_lagging_indicators = df_lagging_indicators.append(df_temp.loc[df_temp['COL0'] == 'CORE_CPI_MoM'],True)
df_lagging_indicators = df_lagging_indicators.append(df_temp.loc[df_temp['COL0'] == 'CORE_PCE_MoM'],True)
df_lagging_indicators = df_lagging_indicators.append(df_temp.loc[df_temp['COL0'] == 'RETAIL_SALES_MoM'],True)
df_lagging_indicators = df_lagging_indicators.append(df_temp.loc[df_temp['COL0'] == 'CORE_RETAIL_SALES_MoM'],True)
df_lagging_indicators = df_lagging_indicators.append(df_temp.loc[df_temp['COL0'] == 'UNEMPLOYMENT_RATE_MoM'],True)
df_lagging_indicators = df_lagging_indicators.append(df_temp.loc[df_temp['COL0'] == 'PAYEMS'],True) #NFP
df_lagging_indicators = df_lagging_indicators.append(df_temp.loc[df_temp['COL0'] == 'ICSA'],True) #Weekly Claims
df_lagging_indicators = df_lagging_indicators.append(df_temp.loc[df_temp['COL0'] == 'INDUSTRIAL_PRODUCTION_MoM'],True)

# Get Original Sheet and store it in a dataframe
#df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)
#df_updated = combine_df_on_index(df_original, df_lagging_indicators, 'COL0')

# Write the updated df back to the excel sheet
#write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, -1)

##################################
# Get US Rates and Currency Data #
##################################
sheet_name = 'DB US Rates and Currency'

# Get Current Fed Funds Current Target Rate
df_current_ffr_target = get_current_ffr_target()

# Calculate Eurodollar Futures quotes for 1m, 6m, 12m
df_eurodollar_futures = get_eurodollar_futures()

# Get US Treasury Yields #
excel_file_path_013 = '/Trading_Excel_Files/02_Interest_Rates_FX/013_Yield_Curve.xlsm'
sheet_name_013 = 'Database'

# Get Original Sheet and store it in a dataframe
df_data_013 = convert_excelsheet_to_dataframe(excel_file_path_013, sheet_name_013, True)

df_us_treasury_yields = get_data(df_data_013)

# Get DXY for Last, 6m, 12m
excel_file_path_001 = '/Trading_Excel_Files/01_Lagging_Coincident_Indicators/001_Lagging_Indicator_YoY_Asset_Class_Performance.xlsm'
sheet_name_001 = 'Database'

# Get Original Sheet and store it in a dataframe
df_data_001 = convert_excelsheet_to_dataframe(excel_file_path_001, sheet_name_001, True)
df_dxy_001 = df_data_001.filter(['DATE','DX-Y.NYB'])

#Rename column
df_dxy_001 = df_dxy_001.rename(columns={"DX-Y.NYB": "DXY"})

df_dxy = get_data(df_dxy_001)

# Temp df to combine all rows
df_us_rates_currency = df_current_ffr_target.append(df_eurodollar_futures, ignore_index=True)
df_us_rates_currency = df_us_rates_currency.append(df_us_treasury_yields, ignore_index=True)
df_us_rates_currency = df_us_rates_currency.append(df_dxy, ignore_index=True)

# Get Original Sheet and store it in a dataframe
#df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)
#df_updated = combine_df_on_index(df_original, df_us_rates_currency, 'COL0')

# Write the updated df back to the excel sheet
#write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, -1)

#############################
# Get US Leading Indicators #
#############################

#sheet_name = 'DB US Leading Indicators'

excel_file_path_015 = '/Trading_Excel_Files/03_Leading_Indicators/015_Leading_Indicator_US_LEI_Consumer_Confidence.xlsm'
sheet_name_015 = 'DB LEI'

# Get Original Sheet and store it in a dataframe
df_data_015 = convert_excelsheet_to_dataframe(excel_file_path_015, sheet_name_015, True)

df_lei_015 = df_data_015.filter(['DATE','LEI']).dropna()
df_umcsi_015 = df_data_015.filter(['DATE','UMCSI']).dropna()
df_exp_015 = df_data_015.filter(['DATE','EXPECTED']).dropna()

df_lei = get_data(df_lei_015)
df_umcsi = get_data(df_umcsi_015)
df_exp = get_data(df_exp_015)

# Building Permits
excel_file_path_020 = '/Trading_Excel_Files/03_Leading_Indicators/020_Leading_Indicator_US_Housing_Market.xlsm'
sheet_name_020 = 'Database New'

# Get Original Sheet and store it in a dataframe
df_permits_020 = convert_excelsheet_to_dataframe(excel_file_path_020, sheet_name_020, True)
df_permits_020 = df_permits_020.filter(['DATE','PERMIT']).dropna()

df_permits = get_data(df_permits_020)

# ISM Manufacturing, ISM Manuf New Orders
excel_file_path_016 = '/Trading_Excel_Files/03_Leading_Indicators/016_Leading_Indicator_US_ISM_Manufacturing.xlsm'
sheet_name_016 = 'DB Details'

# Get Original Sheet and store it in a dataframe
df_ism_man_016 = convert_excelsheet_to_dataframe(excel_file_path_016, sheet_name_016, True)
df_ism_man_016 = df_ism_man_016.filter(['DATE','ISM', 'NEW_ORDERS'])

df_ism_016 = df_ism_man_016.filter(['DATE','ISM']).dropna()
df_new_orders_016 = df_ism_man_016.filter(['DATE','NEW_ORDERS']).dropna()

df_ism = get_data(df_ism_016)
df_new_orders = get_data(df_new_orders_016)

# ISM Services
excel_file_path_017 = '/Trading_Excel_Files/03_Leading_Indicators/017_Leading_Indicator_US_ISM_Services.xlsm'
sheet_name_017 = 'DB Details'

# Get Original Sheet and store it in a dataframe
df_ism_ser_017 = convert_excelsheet_to_dataframe(excel_file_path_017, sheet_name_017, True)
df_ism_ser_017 = df_ism_ser_017.filter(['DATE','ISM_SERVICES']).dropna()
df_ism_ser = get_data(df_ism_ser_017)

# Money Supply M1
df_M1REAL = get_data_fred('M1REAL','M1','M')
df_m1 = get_data(df_M1REAL)

# Money Supply M2
df_M2REAL = get_data_fred('M2REAL','M2','M')
df_m2 = get_data(df_M2REAL)

# Temp df to combine all rows
df_temp = df_lei.append(df_umcsi, ignore_index=True)
df_temp = df_temp.append(df_exp, ignore_index=True)
df_temp = df_temp.append(df_permits, ignore_index=True)
df_temp = df_temp.append(df_ism, ignore_index=True)
df_temp = df_temp.append(df_new_orders, ignore_index=True)
df_temp = df_temp.append(df_ism_ser, ignore_index=True)
df_temp = df_temp.append(df_m1, ignore_index=True)
df_temp = df_temp.append(df_m2, ignore_index=True)

#Select Final Columns
df_us_leading_indicators = df_temp.loc[df_temp['COL0'] == 'GDP_QoQ']
df_us_leading_indicators = df_us_leading_indicators.append(df_temp.loc[df_temp['COL0'] == 'UMCSI'],True)
df_us_leading_indicators = df_us_leading_indicators.append(df_temp.loc[df_temp['COL0'] == 'EXPECTED'],True)
df_us_leading_indicators = df_us_leading_indicators.append(df_temp.loc[df_temp['COL0'] == 'LEI'],True)
df_us_leading_indicators = df_us_leading_indicators.append(df_temp.loc[df_temp['COL0'] == 'PERMIT'],True)
df_us_leading_indicators = df_us_leading_indicators.append(df_temp.loc[df_temp['COL0'] == 'ISM'],True)
df_us_leading_indicators = df_us_leading_indicators.append(df_temp.loc[df_temp['COL0'] == 'NEW_ORDERS'],True)
df_us_leading_indicators = df_us_leading_indicators.append(df_temp.loc[df_temp['COL0'] == 'ISM_SERVICES'],True) 
df_us_leading_indicators = df_us_leading_indicators.append(df_temp.loc[df_temp['COL0'] == 'M1_MoM'],True) 
df_us_leading_indicators = df_us_leading_indicators.append(df_temp.loc[df_temp['COL0'] == 'M2_MoM'],True)

sheet_name = 'DB US Indicators'

# Combine all US Indicator Rows
df_us_indicators = df_lagging_indicators.append(df_us_rates_currency, ignore_index=True)
df_us_indicators = df_us_indicators.append(df_us_leading_indicators, ignore_index=True)

#Make COL0 the index so that we can rename each row
df_us_indicators = df_us_indicators.set_index('COL0')

#Rename Rows to match Excel
df_us_indicators = df_us_indicators.rename(index={'GDP_QoQ':'GDP qoq','GDP_YoY':'GDP yoy','CORE_CPI_MoM':'Core CPI',
                        'CORE_PCE_MoM':'Core PCE','RETAIL_SALES_MoM':'Retail Sales MoM','CORE_RETAIL_SALES_MoM':'Core Retail Sales',
                        'UNEMPLOYMENT_RATE_MoM':'Unemployment Rate','PAYEMS':'Non-Farm Payroll','ICSA':'Weekly Claims',
                        'INDUSTRIAL_PRODUCTION_MoM':'Industrial Production','UMCSI':'UMCSI Index','EXPECTED':'UMCSI Exp',
                        'LEI':'Conference Board LEI','PERMIT':'Building Permits','ISM':'ISM Manufacturing',
                        'NEW_ORDERS':'ISM Manuf New orders','ISM_SERVICES':'ISM Services','M1_MoM':'Money Supply M1',
                        'M2_MoM':'Money Supply M2'})

df_us_indicators.reset_index(level=0, inplace=True)

df_us_indicators = df_us_indicators.rename(columns={"LAST": "Last", "6M": "6m", "12M": "12m"})

# Get Original Sheet and store it in a dataframe
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)
df_updated = combine_df_on_index(df_original, df_us_indicators, 'COL0')


#Reorder columns
# get a list of columns
cols = list(df_updated)

# Move the column to head of list using index, pop and insert
cols.insert(0, cols.pop(cols.index('COL0')))
cols.insert(1, cols.pop(cols.index('Last')))
cols.insert(2, cols.pop(cols.index('6m')))
cols.insert(3, cols.pop(cols.index('12m')))

# Reorder
df_updated = df_updated[cols]

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, -1)

#################################
# Get ISM Manufacturing Sectors #
#################################

sheet_name = 'DB ISM Manufacturing Sectors'

excel_file_path_016 = '/Trading_Excel_Files/03_Leading_Indicators/016_Leading_Indicator_US_ISM_Manufacturing.xlsm'
sheet_name_016 = 'DB Manufacturing ISM'

# Get Original Sheet and store it in a dataframe
df_ism_sectors_016 = convert_excelsheet_to_dataframe(excel_file_path_016, sheet_name_016, True).dropna()
df_ism_sectors_016 = df_ism_sectors_016.tail(6)

# Write the updated df back to the excel sheet. Just overwrite what is already there
write_dataframe_to_excel(excel_file_path, sheet_name, df_ism_sectors_016, False, 0)

###########################
# Get ISM Service Sectors #
###########################

sheet_name = 'DB ISM Services Sectors'

excel_file_path_017 = '/Trading_Excel_Files/03_Leading_Indicators/017_Leading_Indicator_US_ISM_Services.xlsm'
sheet_name_017 = 'DB Services ISM'

# Get Original Sheet and store it in a dataframe
df_ism_sectors_017 = convert_excelsheet_to_dataframe(excel_file_path_017, sheet_name_017, True).dropna()
df_ism_sectors_017 = df_ism_sectors_017.tail(6)

# Write the updated df back to the excel sheet. Just overwrite what is already there
write_dataframe_to_excel(excel_file_path, sheet_name, df_ism_sectors_017, False, 0)

###############################
# Get PMI Manufacturing World #
###############################

sheet_name = 'DB PMI Manufacturing World'

excel_file_path_018 = '/Trading_Excel_Files/03_Leading_Indicators/018_Leading_Indicator_PMI_Manufacturing_World.xlsm'
sheet_name_018 = 'DB Country PMI'

# Load original data from excel file into original df
df_pmi_countries_world_018 = convert_excelsheet_to_dataframe(excel_file_path_018, sheet_name_018, False)
df_pmi_countries_world_018 = df_pmi_countries_world_018.filter(['Date','United States','China','Euro Area','Japan','Germany','France','United Kingdom','Italy','Spain','Brazil','Mexico','Russia','India','Canada','Australia','Indonesia','South Korea','Taiwan','Greece','Ireland','Turkey','Czech Republic','Poland','Denmark']).dropna()
df_pmi_countries_world_018 = df_pmi_countries_world_018.rename(columns={"Date": "DATE"})

# Add Global PMI
sheet_name_018 = 'DB Global PMI'
# Load original data from excel file into original df
df_global_pmi_018 = convert_excelsheet_to_dataframe(excel_file_path_018, sheet_name_018, True)
df_global_pmi_018 = df_global_pmi_018.filter(['DATE', 'Global']).dropna()

# Add US ISM
sheet_name_018 = 'DB US ISM Manufacturing'
# Load original data from excel file into original df
df_us_ism_018 = convert_excelsheet_to_dataframe(excel_file_path_018, sheet_name_018, True)
df_us_ism_018 = df_us_ism_018.filter(['DATE', 'ISM Manufacturing']).dropna()
df_us_ism_018 = df_us_ism_018.rename(columns={"ISM Manufacturing": "US ISM"})

df_pmi_man_world = combine_df_on_index(df_pmi_countries_world_018, df_global_pmi_018, "DATE")
df_pmi_man_world = combine_df_on_index(df_pmi_man_world, df_us_ism_018, "DATE")

df_pmi_man_world = df_pmi_man_world.rename(columns={'US ISM': 'ISM','United States': 'US','China': 'CHN', 'Euro Area': 'EU','Japan': 'JPN','Germany': 'DEU',
                                                'France': 'FRA','United Kingdom': 'UK', 'Italy': 'ITA','Spain': 'ESP', 'Brazil': 'BRA',  
                                                'Mexico': 'MEX', 'Russia': 'RUS',  'India': 'IND', 'Canada': 'CAN', 'Australia': 'AUS', 
                                                'Indonesia': 'IDN', 'South Korea': 'KOR', 'Taiwan': 'TAI', 'Greece': 'GRE', 'Ireland':'IRE', 
                                                'Turkey': 'TUR', 'Czech Republic': 'CZE', 'Poland': 'POL', 'Denmark': 'DEN'}) 

#Reorder columns
# get a list of columns
cols = list(df_pmi_man_world)

# Move the column to head of list using index, pop and insert
cols.insert(0, cols.pop(cols.index('DATE')))
cols.insert(1, cols.pop(cols.index('Global')))
cols.insert(2, cols.pop(cols.index('ISM')))
cols.insert(3, cols.pop(cols.index('US'))) 
cols.insert(4, cols.pop(cols.index('CHN'))) 
cols.insert(5, cols.pop(cols.index('EU')))      
cols.insert(6, cols.pop(cols.index('JPN')))          
cols.insert(7, cols.pop(cols.index('DEU')))                
cols.insert(8, cols.pop(cols.index('FRA')))         
cols.insert(9, cols.pop(cols.index('UK'))) 
cols.insert(10, cols.pop(cols.index('ITA')))          
cols.insert(11, cols.pop(cols.index('ESP'))) 
cols.insert(12, cols.pop(cols.index('BRA')))  
cols.insert(13, cols.pop(cols.index('MEX'))) 
cols.insert(14, cols.pop(cols.index('RUS')))  
cols.insert(15, cols.pop(cols.index('IND')))   
cols.insert(16, cols.pop(cols.index('CAN')))  
cols.insert(17, cols.pop(cols.index('AUS')))      
cols.insert(18, cols.pop(cols.index('IDN'))) 
cols.insert(19, cols.pop(cols.index('KOR')))  
cols.insert(20, cols.pop(cols.index('TAI'))) 
cols.insert(21, cols.pop(cols.index('GRE'))) 
cols.insert(22, cols.pop(cols.index('IRE'))) 
cols.insert(23, cols.pop(cols.index('TUR')))           
cols.insert(24, cols.pop(cols.index('CZE'))) 
cols.insert(25, cols.pop(cols.index('POL'))) 
cols.insert(26, cols.pop(cols.index('DEN')))   
# Reorder
df_pmi_man_world = df_pmi_man_world[cols]

# Get Original Sheet and store it in a dataframe
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)
df_updated = combine_df_on_index(df_original, df_pmi_man_world, 'DATE')

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

# TODO: Install Certificates for:
# www.conference-board.org
# www.ismworld.org
# ecommerce.ismworld.org
# Go Daddy Secure Certificate Authority - G2
print("Done!")