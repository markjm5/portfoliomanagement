import requests
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
from common import convert_excelsheet_to_dataframe, write_dataframe_to_excel
from common import combine_df_on_index, get_yf_data, get_gdp_fred,get_oecd_data
from common import get_ism_manufacturing_content, scrape_ism_manufacturing_headline_index
from common import get_stlouisfed_data

excel_file_path = '/Trading_Excel_Files/03_Leading_Indicators/018_Leading_Indicator_PMI_Manufacturing_World.xlsm'

############################################
# Get US Lagging and Coincident Indicators #
############################################

sheet_name = 'DB US Lagging Indicators'

#US GDP
df_GDPC1 = get_gdp_fred('GDPC1')
#TODO: Get last GDP Number (QoQ, YoY). Then get GDP numbers for 6m and 12m ago from last

#US  Core CPI
df_CPILFESL = get_stlouisfed_data('CPILFESL')

#US  Core PCE
df_PCEPILFE = get_stlouisfed_data('PCEPILFE')

#US Retail Sales Ex Auto and Gas
df_MARTSSM44W72USS = get_stlouisfed_data('MARTSSM44W72USS')

#US Unemployment Rate
df_UNRATE = get_stlouisfed_data('UNRATE')

#US NFP
df_PAYEMS = get_stlouisfed_data('PAYEMS')

#US Weekly Claims
df_ICSA = get_stlouisfed_data('ICSA')

#US Industrial Production
df_INDPRO = get_stlouisfed_data('INDPRO')

##################################
# Get US Rates and Currency Data #
##################################
sheet_name = 'DB US Rates and Currency'



#############################
# Get US Leading Indicators #
#############################





print("Done!")