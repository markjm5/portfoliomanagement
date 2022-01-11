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
from common import combine_df_on_index, get_stlouisfed_data

excel_file_path = '/Trading_Excel_Files/03_Leading_Indicators/015_Leading_Indicator_US_Consumer_Confidence.xlsm'
sheet_name = 'Database'

def scrape_conference_board_lei():
  # https://www.conference-board.org/pdf_free/press/US%20LEI%20PRESS%20RELEASE%20-%20December%202021.pdf
  # https://www.conference-board.org/data/bcicountry.cfm?cid=1

  url_lei = 'https://www.conference-board.org/data/bcicountry.cfm?cid=1'

  # Add additional column to df with China PMI Index
  page = requests.get(url=url_lei,verify=False)
  soup = BeautifulSoup(page.content, 'html.parser')

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

  lei_date = "01/%s/%s" % (lei_date.month, lei_date.year)

  import pdb; pdb.set_trace()


  #TODO: return a dataframe with lei_month and lei_value, as DATE and LEI columns

  return df_lei


#########################################
# Scrape Latest US Conference Board LEI #
#########################################

#Scrape LEI Month, Year and Value from Conference Board monthly article

df_lei = scrape_conference_board_lei()

#Get US GDP
df_GDPC1 = get_stlouisfed_data('GDPC1')

#TODO: Get S&P500 from YF
#TODO: Get UMICI Index

#TODO: Combine them into one df
#TODO: Write to excel sheet

print("Done!")
