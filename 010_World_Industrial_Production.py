import pandas as pd
import requests
import re
from datetime import date
from bs4 import BeautifulSoup
from requests.models import parse_header_links
from common import get_oecd_data, write_to_directory


def scrape_div_capital_investment(url):
  #Scrape GDP Table from https://www.theglobaleconomy.com/rankings/Capital_investment/

  page = requests.get(url=url)
  soup = BeautifulSoup(page.content, 'html.parser')

  #Need to scrape div for capital investment list countries and numbers.
  value = soup.find('input', {'id': 'export_data'}).get('value')
  countries = value.split('=')[4].split('&')[0]
  numbers = value.split('=')[3].split('&')[0]

  #Need to put countries and numbers into a pandas dataframe.
  dataframe_values = {'COUNTRY': countries.split('|'), 'CAPITAL_INVESTMENT': numbers.split('|')}
  df = pd.DataFrame(data=dataframe_values)

  return df

def scrape_table_world_production(url):
  #Scrape GDP Table from https://tradingeconomics.com/country-list/industrial-production?continent=world

  page = requests.get(url=url)
  soup = BeautifulSoup(page.content, 'html.parser')

  #TODO: Need to scrape table for world production countries and numbers.
  table = soup.find('table')
  #import pdb; pdb.set_trace()
  table_rows = table.find_all('tr', attrs={'align':'center'})
  table_rows_header = table.find_all('tr')[0].find_all('th')
  df = pd.DataFrame()
  index = 0
  for header in table_rows_header:
    df.insert(index,str(header.text).strip(),[],True)
    index+=1

  #import pdb; pdb.set_trace()

  return df

def scrape_table_china_production(url):
  #Scrape GDP Table from https://www.investing.com/economic-calendar/chinese-industrial-production-462

  page = requests.get(url=url)
  soup = BeautifulSoup(page.content, 'html.parser')

  #TODO: Need to scrape table for china production and numbers.
  table = soup.find('table')
  table_rows = table.find_all('tr', attrs={'align':'center'})
  table_rows_header = table.find_all('tr')[0].find_all('th')
  df = pd.DataFrame()

  return df


#Get Capital Investment Data
df_capital_investment = scrape_div_capital_investment("https://www.theglobaleconomy.com/rankings/Capital_investment/")
#Write to a csv file in the correct directory
write_to_directory(df_capital_investment,'010_Lagging_Indicator_Capital_Investment.csv')

#Get World Production Data
df_world_production = scrape_table_world_production("https://tradingeconomics.com/country-list/industrial-production?continent=world")
#Write to a csv file in the correct directory
write_to_directory(df_world_production,'010_Lagging_Indicator_World_Production.csv')

#Get China Production Data
#df_china_production = scrape_table_china_production("https://www.investing.com/economic-calendar/chinese-industrial-production-462")
#Write to a csv file in the correct directory
#write_to_directory(df_china_production,'010_Lagging_Indicator_China_Production.csv')


# Get OECD Data Using API: https://stackoverflow.com/questions/40565871/read-data-from-oecd-api-into-python-and-pandas
#Get World Industrial Production
#https://stats.oecd.org/restsdmx/sdmx.ashx/GetData/KEI/PRINTO01.AUS+AUT+BEL+USA.ST.M/all?startTime=1919-01&endTime=2021-11
country = ['AUS','AUT','BEL','CAN','CHL','CZE','DEU','DNK','ESP','EST','FIN','FRA','GBR','GRC','HUN','IRL','ISL','ISR','ITA','JPN','KOR','LUX','LVA','MEX','NLD','NOR','OECD','POL','PRT','SVK','SVN','SWE','NZL','USA','EA19','G-7','CHE','IND','ZAF','RUS','TUR','BRA','ARG']
subject = ['PRINTO01']
measure = ['ST']
frequency = 'M'
startDate = '1919-Q1'

todays_date = date.today()
endDate = '%s-Q4' % (todays_date.year)

df_world_industrial_production = get_oecd_data('KEI', [country, subject, measure, [frequency]], {'startTime': startDate, 'endTime': endDate, 'dimensionAtObservation': 'AllDimensions','filename': '010_World_Industrial_Production.xml'})

#Write to a csv file in the correct directory
write_to_directory(df_world_industrial_production,'010_Lagging_Indicator_World_Indistrial_Production.csv')

print("Done!")