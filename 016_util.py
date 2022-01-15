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
from common import _transform_data

excel_file_path = '/Trading_Excel_Files/03_Leading_Indicators/016_Leading_Indicator_US_ISM_Manufacturing.xlsm'
sheet_name_original = 'DB OLD Production'
sheet_name_new = 'DB Production'

_transform_data(excel_file_path, sheet_name_original, sheet_name_new)

print("Done!")
