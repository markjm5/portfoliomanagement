import requests
import pandas as pd
from datetime import datetime as dt
from dateutil import relativedelta
from bs4 import BeautifulSoup
from requests.models import parse_header_links
from common import get_data_fred, get_sp500_monthly_prices, convert_excelsheet_to_dataframe
from common import combine_df_on_index, write_dataframe_to_excel

excel_file_path = '/Trading_Excel_Files/03_Leading_Indicators/015_Leading_Indicator_US_LEI_Consumer_Confidence.xlsm'
sheet_name = 'DB LEI'

def scrape_conference_board_lei():
  # https://www.conference-board.org/pdf_free/press/US%20LEI%20PRESS%20RELEASE%20-%20December%202021.pdf
  # https://www.conference-board.org/data/bcicountry.cfm?cid=1

  url_lei = 'https://www.conference-board.org/data/bcicountry.cfm?cid=1'

  # Add additional column to df with China PMI Index
  #*page = requests.get(url=url_lei,verify=False)
  page = requests.get(url=url_lei)

  soup = BeautifulSoup(page.content, 'html.parser')

  # Get the date of the article
  date_string = soup.find("p", {"class": "date"}).text[0 + len('Released: '):len(soup.find("p", {"class": "date"}).text)].replace(',','')

  # Convert article date into datetime, and get the previous month because that will be the LEI month
  article_date = dt.strptime(date_string, "%A %B %d %Y")
  lei_date = article_date - relativedelta.relativedelta(months=1)

  paragraph = "" #The para that contains the LEI monthly value
  lei_month_string = lei_date.strftime("%B") #The month before article_date
  lei_value = "" # The LEI value that is extracted from the para

  #get all paragraphs
  paras = soup.find_all("p", attrs={'class': None})

  for para in paras:
    #Get the specific paragraph that contains the LEI value based on whether the month name exists in the para
    if(para.text.startswith('The Conference Board Leading Economic Index® (LEI)') and para.text.find(lei_month_string + ' to') > -1):
        paragraph = para.text

  #Extract LEI value from the paragraph using the Month string
  lei_value = paragraph[paragraph.find(lei_month_string) + len(lei_month_string):paragraph.find(' (2016')].split(' ')[2]  

  df_lei = pd.DataFrame()
  df_lei.insert(0,"DATE",[],True)
  df_lei.insert(1,"LEI",[],True)

  lei_date = "01/%s/%s" % (lei_date.month, lei_date.year)

  # return a dataframe with lei_month and lei_value, as DATE and LEI columns
  df_lei = df_lei.append({'DATE': lei_date, 'LEI': lei_value}, ignore_index=True)

  # format columns in dataframe
  df_lei['DATE'] = pd.to_datetime(df_lei['DATE'],format='%d/%m/%Y')
  df_lei['LEI'] = pd.to_numeric(df_lei['LEI'])

  return df_lei

#########################################
# Scrape Latest US Conference Board LEI #
#########################################

#Scrape LEI Month, Year and Value from Conference Board monthly article
df_LEI = scrape_conference_board_lei()

#################################
# Get US GDP from St Louis FRED #
#################################

#Get US GDP
#df_GDPC1 = get_gdp_fred('GDPC1')
df_GDPC1 = get_data_fred('GDPC1', 'GDPC1', 'Q')

#df_GDPC1 = get_stlouisfed_data('GDPC1')

#df_GDPC1['GDPQoQ'] = (df_GDPC1['GDPC1'] - df_GDPC1['GDPC1'].shift()) / df_GDPC1['GDPC1']
#df_GDPC1['GDPYoY'] = (df_GDPC1['GDPC1'] - df_GDPC1['GDPC1'].shift(periods=4)) / df_GDPC1['GDPC1']

###########################################
# Get S&P500 Monthly Close Prices from YF #
###########################################

df_SP500 = get_sp500_monthly_prices()

#get date range
#todays_date = date.today()
#date_str = "%s-%s-%s" % (todays_date.year, todays_date.month, "01")

# Get S&P500 close month intervals using above date range
#df_SP500 = get_yf_data("^GSPC", "1mo", "1959-01-01", date_str)

#Remove unnecessary columns from df_SP500 and rename columns
#df_SP500 = df_SP500.drop(['Open', 'High', 'Low', 'Volume'], axis=1)
#df_SP500 = df_SP500.rename(columns={"Close": "SP500"})

########################
# Get UMCSI Index Data #
########################

# Get UMCSI Index
#df_UMCSENT = get_stlouisfed_data('UMCSENT')
df_UMCSI = pd.read_csv('http://www.sca.isr.umich.edu/files/tbmics.csv')
df_UMCSI["YYYY"] = df_UMCSI["YYYY"].apply(str)
df_UMCSI["DATE"] = pd.to_datetime(df_UMCSI["YYYY"] + "-" + df_UMCSI["Month"] + "-01", format='%Y-%B-%d')
df_UMCSI = df_UMCSI.drop(['Month', 'YYYY'], axis=1)
df_UMCSI = df_UMCSI.rename(columns={"ICS_ALL": "UMCSI"})

# get a list of columns
cols = list(df_UMCSI)
# move the column to head of list
cols.insert(0, cols.pop(cols.index('DATE')))

# reorder
df_UMCSI = df_UMCSI[cols]

# Summary Indexes
df_UM_SUMMARY = pd.read_csv('http://www.sca.isr.umich.edu/files/tbmiccice.csv')
df_UM_SUMMARY["YYYY"] = df_UM_SUMMARY["YYYY"].apply(str)
df_UM_SUMMARY["DATE"] = pd.to_datetime(df_UM_SUMMARY["YYYY"] + "-" + df_UM_SUMMARY["Month"] + "-01", format='%Y-%B-%d')
df_UM_SUMMARY = df_UM_SUMMARY.drop(['Month', 'YYYY'], axis=1)
df_UM_SUMMARY = df_UM_SUMMARY.rename(columns={"ICC": "CURRENT", "ICE": "EXPECTED"})

# get a list of columns
cols = list(df_UM_SUMMARY)
# move the column to head of list
cols.insert(0, cols.pop(cols.index('DATE')))

# reorder
df_UM_SUMMARY = df_UM_SUMMARY[cols]

# Combine df_LEI, df_GDPC1, df_SP500 and df_UMCSI into original df
df = combine_df_on_index(df_LEI, df_GDPC1, 'DATE')
df = combine_df_on_index(df_SP500, df, 'DATE')
df = combine_df_on_index(df_UMCSI, df, 'DATE')
df = combine_df_on_index(df_UM_SUMMARY, df, 'DATE')

# Load original data from excel file into original df
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

#Combine new data with original data
df_updated = combine_df_on_index(df_original, df, 'DATE')

# get a list of columns
cols = list(df_updated)
# move the column to head of list
cols.insert(0, cols.pop(cols.index('DATE')))
cols.insert(1, cols.pop(cols.index('LEI')))
cols.insert(2, cols.pop(cols.index('SP500')))
cols.insert(3, cols.pop(cols.index('UMCSI')))
cols.insert(4, cols.pop(cols.index('CURRENT')))
cols.insert(5, cols.pop(cols.index('EXPECTED')))
cols.insert(6, cols.pop(cols.index('GDPC1')))
cols.insert(7, cols.pop(cols.index('GDPQoQ')))
cols.insert(8, cols.pop(cols.index('GDPYoY')))
cols.insert(9, cols.pop(cols.index('GDPQoQ_ANNUALIZED')))

# reorder
df_updated = df_updated[cols]

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

print("Done!")
