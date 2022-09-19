from unittest import skip
import pandas as pd
import re
from datetime import date
from common import get_stlouisfed_data, convert_excelsheet_to_dataframe, write_dataframe_to_excel
from common import combine_df_on_index, get_page,convert_html_table_to_df
from bs4 import BeautifulSoup

excel_file_path = '/Trading_Excel_Files/04_Filtering_Process/031_Filtering_Process_Idea_Generation_Bottom_Up.xlsm'

todays_date = date.today()
year_str = todays_date.year

def scrape_table_sec():
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
    df = pd.DataFrame()

    # Get earnings calendar for the next fortnight
    for x in range(1, 10):
        earnings_whispers_day_df = scrape_earningswhispers_day(x)
        df = df.append(earnings_whispers_day_df, ignore_index=True)

    return df

def scrape_earningswhispers_day(day):
    url = "https://www.earningswhispers.com/calendar?sb=c&d=%s&t=all" % (day,)

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

    skip_first = True

    for tr in table_rows:        
        temp_row = []

        td = tr.find_all('div')

        #TODO: Just Extract Date, Time, CompanyName, Ticker
        for obs in td:  
            text = str(obs.text).strip()
            temp_row.append(text)    

        time_str = temp_row[4]
        company_name_str = temp_row[2]
        ticker_str = temp_row[3]

        if(time_str.find(' ET') != -1):
            #TODO: Get market cap from US Stocks list
            #TODO: Only if company exists on US stocks list, we add to df

            temp_row1 = []
            temp_row1.append(date_str)
            temp_row1.append(time_str)
            temp_row1.append(ticker_str)
            temp_row1.append(company_name_str)

            if not skip_first:   
                df.loc[len(df.index)] = temp_row1

        skip_first = False

    return df

sheet_name = 'Spin Off'
#df_spin_off = scrape_table_sec()

# Write the updated df to the excel sheet, and overwrite what was there before
#write_dataframe_to_excel(excel_file_path, sheet_name, df_spin_off, False, 0, True)

sheet_name = 'Economic Calendar'
#df_economic_calendar = scrape_table_marketscreener_economic_calendar()

# Write the updated df to the excel sheet, and overwrite what was there before
#write_dataframe_to_excel(excel_file_path, sheet_name, df_economic_calendar, False, 0, True)


sheet_name = 'Earnings Calendar'
df_earnings_calendar = scrape_table_earningswhispers_earnings_calendar()

# Write the updated df to the excel sheet, and overwrite what was there before
write_dataframe_to_excel(excel_file_path, sheet_name, df_earnings_calendar, False, 0, True)

print("Done!")
