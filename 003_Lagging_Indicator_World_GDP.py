from json.decoder import JSONDecodeError
import sys
import requests
import os.path
import csv
import pandas as pd
import xml.etree.ElementTree as ET
from inspect import getmembers, isclass, isfunction
from datetime import date
from bs4 import BeautifulSoup
from requests.models import parse_header_links
import re
from common import get_oecd_data, write_to_directory


#TODO: Get OECD Data Using API: https://stackoverflow.com/questions/40565871/read-data-from-oecd-api-into-python-and-pandas
# https://stats.oecd.org/restsdmx/sdmx.ashx/GetData/QNA/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LTU+LVA+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+EA19+EU27_2020+G-7+NAFTA+OECDE+G-20+OECD+ARG+BRA+BGR+CHN+IND+IDN+ROU+RUS+SAU+ZAF.B1_GE+P31S14_S15+P3S13+P51+P52_P53+B11+P6+P7.GYSA+GPSA+CTQRGPSA.Q/all?startTime=2019-Q3&endTime=2021-Q3
"""
def get_data(dataset, dimensions, params):

  dim_args = ['+'.join(d) for d in dimensions]
  dim_str = '.'.join(dim_args)

  url = "https://stats.oecd.org/restsdmx/sdmx.ashx/GetData/%s/%s/all?startTime=%s&endTime=%s" % (dataset, dim_str,params['startTime'],params['endTime'])

  try:

    #resp = requests.get(url=url,params=params)
    resp = requests.get(url=url)

    resp_formatted = resp.text[resp.text.find('<'):len(resp.text)]

    # Write response to an XML File
    with open(params['filename'], 'w') as f:
      f.write(resp_formatted)

    # Load in the XML file into ElementTree
    tree = ET.parse(params['filename'])

    #TODO: Load into a dataframe and return the data frame
    root = tree.getroot()

    ns = {'sdmx': 'http://www.SDMX.org/resources/SDMXML/schemas/v2_0/generic'}

    df = pd.DataFrame()

    #Add column headers
    df.insert(0,"QTR",[],True)
    df.insert(1,"DATE",[],True)
    index = 2
    for series in root.findall('./sdmx:DataSet/sdmx:Series',ns):
      current_country = ""
      for value in series.findall('./sdmx:SeriesKey/sdmx:Value',ns): 
        if(value.get('concept')) == 'LOCATION':
          current_country = value.get('value')
          
          df.insert(index,value.get('value'),[],True)
      index+=1

    #Add Dates  
    quarter_list = []
    date_list = []
    year_start, qtr_start =  params['startTime'].split('-Q')
    year_end, qtr_end =  params['endTime'].split('-Q')

    for x in range(int(year_start), int(year_end)+1):
      for y in range(1,5):

        qtr_string = "%s-Q%s" % (x, y)
        quarter_list.append(qtr_string)
        match y:
          case 1:
            full_date = "1/4/" + str(x)
          case 2:
            full_date = "1/7/" + str(x)

          case 3:
            full_date = "1/10/" + str(x)
          case 4:
            full_date = "1/1/" + str(x)

        date_list.append(full_date)

    df['QTR'] = quarter_list
    df['DATE'] = date_list

    #Add observations for all countries
    for series in root.findall('./sdmx:DataSet/sdmx:Series',ns):
      for value in series.findall('./sdmx:SeriesKey/sdmx:Value',ns): 
        if(value.get('concept')) == 'LOCATION':
          current_country = value.get('value')

          observations = series.findall('sdmx:Obs',ns)

          for observation in observations:
            #TODO: Get qtr, date and observation. Add to column in chronological order for all countries

            obs_qtr = observation.findall('sdmx:Time',ns)[0].text
            obs_row = df.index[df['QTR'] == obs_qtr].tolist()
            obs_value = observation.findall('sdmx:ObsValue',ns)[0].get('value')

            #match based on quarter and country, then add the observation value
            df.loc[obs_row, current_country] = round(float(obs_value),9)        

  except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    print(exc_type, fname, exc_tb.tb_lineno)

  return df
"""

def scrape_table(url):
  #Scrape GDP Table from Trading Economics
  #url = "https://tradingeconomics.com/matrix"

  page = requests.get(url=url)
  soup = BeautifulSoup(page.content, 'html.parser')

  table = soup.find('table')
  table_rows = table.find_all('tr', attrs={'align':'center'})
  table_rows_header = table.find_all('tr')[0].find_all('th')
  df = pd.DataFrame()

  index = 0
  for header in table_rows_header:
    if(index == 0):
      df.insert(0,"Country",[],True)
    else:
      df.insert(index,header.text,[],True)

    index+=1

  #Insert New Row. Format the data to show percentage as float

  for tr in table_rows:
    temp_row = []
    first_col = True

    td = tr.find_all('td')
    for obs in td:
      if(first_col):
        text = ''.join(e for e in obs.text if e.isalnum())
        text = re.sub("([A-Z])", " \\1", text).strip()
      else:
        if(obs.text.find('%') < 0):
          text = obs.text
        else:
          text = obs.text.strip('%')
          text = float(text.strip('%'))/100

      temp_row.append(text)        
      first_col = False

    df.loc[len(df.index)] = temp_row

  return df


#https://stats.oecd.org/restsdmx/sdmx.ashx/GetData/QNA/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LTU+LVA+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+EA19+EU27_2020+G-7+NAFTA+OECDE+G-20+OECD+ARG+BRA+BGR+CHN+IND+IDN+ROU+RUS+SAU+ZAF.B1_GE+P31S14_S15+P3S13+P51+P52_P53+B11+P6+P7.GYSA+GPSA+CTQRGPSA.Q/all?startTime=2019-Q3&endTime=2021-Q3

#Get QoQ Data from OECD
country = ['AUS','AUT','BEL','CAN','CHL','COL','CRI','CZE','DNK','EST','FIN','FRA','DEU','GRC','HUN','ISL','IRL','ISR','ITA','JPN','KOR','LTU','LVA','LUX','MEX','NLD','NZL','NOR','POL','PRT','SVK','SVN','ESP','SWE','CHE','TUR','GBR','USA','EA19','EU27_2020','G-7','NAFTA','OECDE','G-20','OECD','ARG','BRA','BGR','CHN','IND','IDN','ROU','RUS','SAU','ZAF']
#subject = ['B1_GE','P31S14_S15','P3S13','P51','P52_P53','B11','P6','P7']
subject = ['B1_GE']
measure = ['GPSA']
frequency = 'Q'
startDate = '1947-Q1'

todays_date = date.today()
endDate = '%s-Q4' % (todays_date.year)

df_QoQ = get_oecd_data('QNA', [country, subject, measure, [frequency]], {'startTime': startDate, 'endTime': endDate, 'dimensionAtObservation': 'AllDimensions','filename': '003_QoQ.xml'})

#Write to a csv file in the correct directory
write_to_directory(df_QoQ,'003_Lagging_Indicator_World_GDP_QoQ.csv')

#Get YoY Data from OECD
#https://stats.oecd.org/restsdmx/sdmx.ashx/GetData/QNA/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LTU+LVA+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+EA19+EU27_2020+G-7+NAFTA+OECDE+G-20+OECD+ARG+BRA+BGR+CHN+IND+IDN+ROU+RUS+SAU+ZAF.B1_GE+P31S14_S15+P3S13+P51+P52_P53+B11+P6+P7.GYSA+GPSA+CTQRGPSA.Q/all?startTime=2019-Q3&endTime=2021-Q3

country = ['AUS','AUT','BEL','CAN','CHL','COL','CRI','CZE','DNK','EST','FIN','FRA','DEU','GRC','HUN','ISL','IRL','ISR','ITA','JPN','KOR','LTU','LVA','LUX','MEX','NLD','NZL','NOR','POL','PRT','SVK','SVN','ESP','SWE','CHE','TUR','GBR','USA','EA19','EU27_2020','G-7','NAFTA','OECDE','G-20','OECD','ARG','BRA','BGR','CHN','IND','IDN','ROU','RUS','SAU','ZAF']
#subject = ['B1_GE','P31S14_S15','P3S13','P51','P52_P53','B11','P6','P7']
subject = ['B1_GE']
measure = ['GYSA']
frequency = 'Q'
startDate = '1947-Q1'

todays_date = date.today()
endDate = '%s-Q4' % (todays_date.year)

df_YoY = get_oecd_data('QNA', [country, subject, measure, [frequency]], {'startTime': startDate, 'endTime': endDate, 'dimensionAtObservation': 'AllDimensions','filename': '003_YoY.xml'})

#Write to a csv file in the correct directory
write_to_directory(df_YoY,'003_Lagging_Indicator_World_GDP_YoY.csv')


df_World_GDP = scrape_table("https://tradingeconomics.com/matrix")
import pdb; pdb.set_trace()
#Write to a csv file in the correct directory
write_to_directory(df_World_GDP,'003_Lagging_Indicator_World_GDP.csv')

print("Done!")
