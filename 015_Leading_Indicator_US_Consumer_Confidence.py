from json.decoder import JSONDecodeError
import sys
import requests
import os.path
import csv
import calendar
import re
import pandas as pd
import xml.etree.ElementTree as ET
from inspect import getmembers, isclass, isfunction
from datetime import datetime as dt
from dateutil import parser, relativedelta
from datetime import date
from bs4 import BeautifulSoup
from requests.models import parse_header_links
from common import get_oecd_data, get_invest_data, convert_excelsheet_to_dataframe, write_dataframe_to_excel, combine_df, util_check_diff_list
from common import combine_df_on_index, get_stlouisfed_data, get_yf_data

excel_file_path = '/Trading_Excel_Files/03_Leading_Indicators/015_Leading_Indicator_US_Consumer_Confidence.xlsm'
sheet_name = 'Database'

def scrape_conference_board_lei():
  # https://www.conference-board.org/pdf_free/press/US%20LEI%20PRESS%20RELEASE%20-%20December%202021.pdf
  # https://www.conference-board.org/data/bcicountry.cfm?cid=1

  url_lei = 'https://www.conference-board.org/data/bcicountry.cfm?cid=1'

  # Add additional column to df with China PMI Index
  page = requests.get(url=url_lei,verify=False)
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
df_GDPC1 = get_stlouisfed_data('GDPC1')

###########################################
# Get S&P500 Monthly Close Prices from YF #
###########################################

#get date range
todays_date = date.today()
date_str = "%s-%s-%s" % (todays_date.year, todays_date.month, "01")

# Get S&P500 close month intervals using above date range
df_SP500 = get_yf_data("^GSPC", "1mo", "1959-01-01", date_str)

#Remove unnecessary columns from df_SP500 and rename columns
df_SP500 = df_SP500.drop(['Open', 'High', 'Low', 'Volume'], axis=1)
df_SP500 = df_SP500.rename(columns={"Close": "SP500"})

########################
# Get UMCSI Index Data #
########################

#TODO: Get UMCSI Index

import pdb; pdb.set_trace()

#TODO: Load original data from excel file into original df

#TODO: Combine df_LEI, df_GDPC1, df_SP500 and df_UMCSI into original df

#TODO: Write to excel sheet

print("Done!")
