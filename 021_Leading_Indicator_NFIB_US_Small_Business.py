import requests
import os.path
import csv
import pandas as pd
from bs4 import BeautifulSoup
from common import get_stlouisfed_data, convert_excelsheet_to_dataframe, write_dataframe_to_excel
from common import combine_df_on_index
import json as js

excel_file_path = '/Trading_Excel_Files/03_Leading_Indicators/021_Leading_Indicator_NFIB_US_Small_Business.xlsm'
sheet_name = 'Database'

def get_latest_nfib_data(obj):

    url = "http://open.api.nfib-sbet.org/rest/sbetdb/_proc/getIndicator"

    response = requests.post(url=url, data=obj)

    print(response.text)

    #TODO: Text API call in postman
    import pdb; pdb.set_trace()


myobj = { 
            "app_name": "sbet",
            "params": [
                { "name": "minYear", "param_type": "IN", "value": 2010 },
                { "name": "minMonth", "param_type": "IN", "value": 6 },
                { "name": "maxYear", "param_type": "IN", "value": 2010 },
                { "name": "maxMonth", "param_type": "IN", "value": 1 },
                { "name": "indicator", "param_type": "IN", "value": "OPT_INDEX" }
            ]
        }

#TODO: Download all excel files and consolodate them into 'Database' tab
get_latest_nfib_data(myobj)

print("Done!")
