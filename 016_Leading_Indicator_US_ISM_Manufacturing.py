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

excel_file_path = '/Trading_Excel_Files/03_Leading_Indicators/016_Leading_Indicator_US_ISM_Manufacturing.xlsm'
sheet_name = 'DB ISM'

def scrape_pmi_manufacturing_index():

    #get date range
    todays_date = date.today()

    #use todays date to get pmi month (last month) and use the month string to call the url
    pmi_date = todays_date - relativedelta.relativedelta(months=1)
    pmi_month = pmi_date.strftime("%B").lower()
    url_pmi = 'https://www.ismworld.org/supply-management-news-and-reports/reports/ism-report-on-business/pmi/%s' % (pmi_month,)

    page = requests.get(url=url_pmi,verify=False)
    soup = BeautifulSoup(page.content, 'html.parser')

    import pdb; pdb.set_trace()


scrape_pmi_manufacturing_index()

#TODO: Get ISM data for the following tabs:
#Sectors Trend
#Details
#ISM Manufacturing vs GDP
#ISM Manufacturing vs SPX
#ISM Manufacturing
#Industry Comments ?

print("Done!")
