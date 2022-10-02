from unittest import skip
import pandas as pd
import re
from datetime import date
from common import get_stlouisfed_data, convert_excelsheet_to_dataframe, write_dataframe_to_excel
from common import combine_df_on_index, get_page,convert_html_table_to_df, get_zacks_us_companies
from bs4 import BeautifulSoup

excel_file_path = '/Trading_Excel_Files/04_Filtering_Process/031_Filtering_Process_Idea_Generation_Bottom_Up.xlsm'

todays_date = date.today()
year_str = todays_date.year
df_us_companies = get_zacks_us_companies()

def scrape_table_sec():
    print("Getting data from SEC")
    url = "https://www.sec.gov/cgi-bin/srch-edgar?text=form-type+%%3D+10-12b&first=%s&last=%s" % (year_str,year_str)

    page = get_page(url)
    soup = BeautifulSoup(page.content, 'html.parser')

    tables = soup.find_all('table', recursive=True)
    table = tables[4]

    df_sec = convert_html_table_to_df(table, False)

    #Drop unnecessary columns
    df_sec = df_sec.drop(columns='No.', axis=1)
    df_sec = df_sec.drop(columns='Format', axis=1)
    df_sec = df_sec.drop(columns='Size', axis=1)

    #df_sec = df_sec.rename(columns={"Company": "COMPANY","Form Type":"FORM_TYPE", "Filing Date": "FILING_DATE"})

    df_sec['Filing Date'] = pd.to_datetime(df_sec['Filing Date'],format='%m/%d/%Y')

    return df_sec

def scrape_table_marketscreener_economic_calendar():
    print("Getting data from Market Screener")

    url = "https://www.marketscreener.com/stock-exchange/calendar/economic/"

    page = get_page(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    df = pd.DataFrame()

    tables = soup.find_all('table', recursive=True)

    table = tables[6]

    table_rows = table.find_all('tr')

    table_header = table_rows[0]
    td = table_header.find_all('td')
    index = 0

    for obs in td:        
        text = str(obs.text).strip()
        df.insert(index,str(obs.text).strip(),[],True)
        index+=1

    index = 0
    skip_first = True
    session = ""

    for tr in table_rows:        
        temp_row = []
                
        td = tr.find_all('td')
        
        if(len(td) == 5):
            #if not skip_first:
            session = str(td[0].text).strip()
        else:
            temp_row.append(session)        
        
        for obs in td:  
            text = str(obs.text).strip()
            if(text == ''):
                #Maybe this is the country field, which means that the country is represented by a flag image
                try:
                    if(obs.attrs['class'][0] == 'pays'):
                        if(obs.findChild('img')):
                            text = obs.findChild('img').attrs['src']
                            # Format text to remove image and convert into country
                            pattern_regex = re.compile(r'([a-z]*(?=[.]png))')                            
                            text = re.search(pattern_regex,text).group(0)
                            text = text.upper()

                        else:
                            text = str(obs.text).strip()
                except KeyError as e:
                    pass

            temp_row.append(text)        

            if not skip_first:   
                if(len(temp_row) == len(df.columns)):
                    df.loc[len(df.index)] = temp_row
               
        skip_first = False
    df['Session'] = df['Session'].str.replace('\n\n','|')
    df['Session'] = df['Session'].str.replace('\n','|')

    return df

def scrape_table_earningswhispers_earnings_calendar():
    print("Getting data from Earnings Whispers")

    df = pd.DataFrame()

    # Get earnings calendar for the next fortnight
    for x in range(1, 10):
        earnings_whispers_day_df = scrape_earningswhispers_day(x)
        df = df.append(earnings_whispers_day_df, ignore_index=True)

    return df

def scrape_earningswhispers_day(day):
    url = "https://www.earningswhispers.com/calendar?sb=c&d=%s&t=all" % (day,)
    #df_us_companies = get_zacks_us_companies()

    page = get_page(url)
    soup = BeautifulSoup(page.content, 'html.parser')

    date_str = soup.find('div', attrs={"id":"calbox"})
    date_str = date_str.text.strip().replace('for ','')

    eps_cal_table = soup.find('ul', attrs={"id":"epscalendar"})

    table_rows = eps_cal_table.find_all('li')

    df = pd.DataFrame()
    
    # Add Date, Time, CompanyName, Ticker headers to dataframe
    df.insert(0,"Date",[],True)
    df.insert(1,"Time",[],True)
    df.insert(2,"Ticker",[],True)
    df.insert(3,"Company Name",[],True)
    df.insert(4,"Market Cap (Mil)",[],True)
    df.insert(5,"EPS",[],True)
    df.insert(6,"Revenue",[],True)
    df.insert(7,"Expected Revenue Growth",[],True)

    skip_first = True

    for tr in table_rows:        
        temp_row = []

        td = tr.find_all('div')

        # Just Extract Date, Time, CompanyName, Ticker, EPS, Revenue, Expected Revenue
        for obs in td:  
            text = str(obs.text).strip()
            """
            if('class' in obs.attrs):
                if(obs.attrs['class'][0] == 'revgrowth'):
                    for child in obs.children:
                        pattern_regex = re.compile(r'((?!CBRL\",)\"[0-9,.]*%")')  
                        regex_search = re.search(pattern_regex,child.text)

                        if(regex_search):                   
                            text = regex_search.group(0)
            """
            temp_row.append(text)    
        #import pdb; pdb.set_trace()
        time_str = temp_row[4]
        company_name_str = temp_row[2]
        ticker_str = temp_row[3]
        eps = temp_row[7]
        revenue = temp_row[8]
        expected = temp_row[9]

        if(time_str.find(' ET') != -1):
            # Only if company exists on US stocks list, we add to df
            df_retrieved_company_data = df_us_companies.loc[df_us_companies['TICKER'] == ticker_str].reset_index(drop=True)
            if(df_retrieved_company_data.shape[0] > 0):
                temp_row1 = []
                temp_row1.append(date_str)
                temp_row1.append(time_str)
                temp_row1.append(ticker_str)
                temp_row1.append(company_name_str)
                # Get market cap from US Stocks list
                temp_row1.append(df_retrieved_company_data['MARKET_CAP'].iloc[0])
                temp_row1.append(eps)
                temp_row1.append(revenue)
                temp_row1.append(expected)

                #Get EPS, Revenue and Expected Growth

                if not skip_first:   
                    df.loc[len(df.index)] = temp_row1

        skip_first = False

    return df

sheet_name = 'Spin Off'
df_spin_off = scrape_table_sec()

# Write the updated df to the excel sheet, and overwrite what was there before
write_dataframe_to_excel(excel_file_path, sheet_name, df_spin_off, False, 0, True)

sheet_name = 'Economic Calendar'
df_economic_calendar = scrape_table_marketscreener_economic_calendar()
df_economic_calendar = df_economic_calendar.drop_duplicates()

# Write the updated df to the excel sheet, and overwrite what was there before
write_dataframe_to_excel(excel_file_path, sheet_name, df_economic_calendar, False, 0, True)

sheet_name = 'Earnings Calendar'
df_earnings_calendar = scrape_table_earningswhispers_earnings_calendar()
df_earnings_calendar = df_earnings_calendar.drop_duplicates('Ticker')

# Write the updated df to the excel sheet, and overwrite what was there before
write_dataframe_to_excel(excel_file_path, sheet_name, df_earnings_calendar, False, 0, True)

#TODO: Filter different metrics from US Stocks data list, and write to excel
#import pdb; pdb.set_trace()
#df_retrieved_company_data = df_us_companies.loc[df_us_companies['TICKER'] == ticker_str].reset_index(drop=True)

# FCF 2015	FCF 2016	FCF 2017	FCF 2018
# EPS 2015	EPS 2016	EPS 2017	EPS 2018
# Sales 2018	Sales 2019	Sales 2020
# ROE
# AAll

print("Done!")
