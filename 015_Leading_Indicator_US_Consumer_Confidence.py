from json.decoder import JSONDecodeError
import sys
import requests
import os.path
import csv
import calendar
import pandas as pd
import xml.etree.ElementTree as ET
from inspect import getmembers, isclass, isfunction
from datetime import datetime as dt
from dateutil import parser
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

  paragraph = "" #The para that contains the LEI monthly value
  lei_month = "" #The month extrated from the para
  lei_value = "" # The LEI value that is extracted from the para

  # get the correct month from the correct paragraph
  for para in soup.find_all("p"):
    if(para.text.startswith('The Conference Board Leading Economic Index® (LEI)')):
        for word in para.text.split(' '):
            try:
                dt.strptime(word, '%B').month 

                #This word is the month of the LEI release. So get the month name, and grab the whole para as well 
                #Because it will contain the LEI value
                lei_month = word  
                paragraph = para.text
                break
            except ValueError as e:
                pass

  #Extract LEI value from the paragraph using the Month substring
  lei_value = paragraph[paragraph.find(lei_month) + len(lei_month):paragraph.find(' (2016')].split(' ')[2]  

  print(lei_month)
  print(lei_value)      

  df_lei = pd.DataFrame()

  import pdb; pdb.set_trace()

  #TODO: return a dataframe with lei_month and lei_value

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
