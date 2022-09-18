from unittest import skip
import pandas as pd
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

    df_sec = df_sec.rename(columns={"Company": "COMPANY","Form Type":"FORM_TYPE", "Filing Date": "FILING_DATE"})

    df_sec['FILING_DATE'] = pd.to_datetime(df_sec['FILING_DATE'],format='%m/%d/%Y')

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
                try:
                    if(obs.attrs['class'][0] == 'pays'):
                        if(obs.findChild('img')):
                            text = obs.findChild('img').attrs['src']
                            #TODO: Format text to remove image and convert into country
                        else:
                            text = str(obs.text).strip()
                except KeyError as e:
                    pass

            temp_row.append(text)        

            if not skip_first:   
                if(len(temp_row) == len(df.columns)):
                    df.loc[len(df.index)] = temp_row
               
        skip_first = False

    import pdb; pdb.set_trace()

    #Drop unnecessary columns
    df_economic_calendar = df_economic_calendar.drop(columns='No.', axis=1)
    df_economic_calendar = df_economic_calendar.drop(columns='Format', axis=1)
    df_economic_calendar = df_economic_calendar.drop(columns='Size', axis=1)

    df_economic_calendar = df_economic_calendar.rename(columns={"Company": "COMPANY","Form Type":"FORM_TYPE", "Filing Date": "FILING_DATE"})

    df_economic_calendar['FILING_DATE'] = pd.to_datetime(df_economic_calendar['FILING_DATE'],format='%m/%d/%Y')

    return df_economic_calendar


sheet_name = 'Spin Off'
#df_spin_off = scrape_table_sec()

# Write the updated df to the excel sheet, and overwrite what was there before
#write_dataframe_to_excel(excel_file_path, sheet_name, df_spin_off, False, 0)

#sheet_name = 'Economic Calendar Weekly'
#https://www.marketscreener.com/stock-exchange/calendar/economic/

df_economic_calendar = scrape_table_marketscreener_economic_calendar()


#sheet_name = 'Earnings Calendar'
#https://www.earningswhispers.com/calendar?sb=c&d=1&t=all

#df_PERMIT = get_stlouisfed_data('PERMIT')
#df_HOUST = get_stlouisfed_data('HOUST')
#df_COMPUTSA = get_stlouisfed_data('COMPUTSA')

#Combine all these data frames into a single data frame based on the DATE field

#df = combine_df_on_index(df_PERMIT,df_HOUST,"DATE")
#df = combine_df_on_index(df_COMPUTSA,df,"DATE")

# Get Original Sheet and store it in a dataframe
#df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

#df_updated = combine_df_on_index(df_original, df, 'DATE')

# Write the updated df back to the excel sheet
#write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

print("Done!")
