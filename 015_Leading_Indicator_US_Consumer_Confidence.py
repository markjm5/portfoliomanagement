from json.decoder import JSONDecodeError
import sys
import requests
import os.path
import csv
import pandas as pd
import xml.etree.ElementTree as ET
from inspect import getmembers, isclass, isfunction
from datetime import datetime as dt
from datetime import date
from bs4 import BeautifulSoup
from requests.models import parse_header_links
from common import get_oecd_data, get_invest_data, convert_excelsheet_to_dataframe, write_dataframe_to_excel, combine_df, util_check_diff_list
from common import combine_df_on_index, get_stlouisfed_data

excel_file_path = '/Trading_Excel_Files/03_Leading_Indicators/015_Leading_Indicator_US_Consumer_Confidence.xlsm'

#########################################
# Scrape Latest US Conference Board LEI #
#########################################

sheet_name = 'Database'

#TODO: Scrape Month, Year and Value:
# https://www.conference-board.org/pdf_free/press/US%20LEI%20PRESS%20RELEASE%20-%20December%202021.pdf
# https://www.conference-board.org/data/bcicountry.cfm?cid=1


#Get US GDP
df_GDPC1 = get_stlouisfed_data('GDPC1')

#TODO: Get S&P500 from YF
#TODO: Get UMICI Index
#TODO: Combine them into one df
#TODO: Write to excel sheet

print("Done!")
