import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import date
from common import convert_excelsheet_to_dataframe, write_dataframe_to_excel
from common import combine_df_on_index

excel_file_path = '/Trading_Excel_Files/02_Interest_Rates_FX/013_Yield_Curve.xlsm'
sheet_name = 'Database'

def get_us_treasury_yields():
  # https://home.treasury.gov/resource-center/data-chart-center/interest-rates/TextView?type=daily_treasury_yield_curve&field_tdr_date_value_month=202202

  filename = '013_Daily_Treasury_Yields.xml'

  todays_date = date.today()
  date_str = "%s%s" % (todays_date.strftime('%Y'), todays_date.strftime('%m'))

  url = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml?data=daily_treasury_yield_curve&field_tdr_date_value_month=%s" % (date_str,)

  file_path = '~/Documents/PythonProjects/PortfolioManagement/XML/%s' % filename 
  try:
      resp = requests.get(url=url)

      resp_formatted = resp.text[resp.text.find('<'):len(resp.text)]
      # Write response to an XML File
      with open(file_path, 'w') as f:
          f.write(resp_formatted)

  except requests.exceptions.ConnectionError:
      print("Connection refused, Opening from File...")

  # Load in the XML file into ElementTree
  tree = ET.parse(file_path)
  data = {'DATE': [], '3M':[], '2Y': [], '3Y': [], '10Y': [], '30Y': []}
  df_us_treasury_yields = pd.DataFrame(data=data)

  #Load into a dataframe and return the data frame
  root = tree.getroot()

  ns = {'ty': 'http://www.w3.org/2005/Atom'}

  # <class 'xml.etree.ElementTree.Element'>
  for content in root.findall('./ty:entry/ty:content',ns):
    temp_row = []

    for elem in content.iter():
      #Check if current tag is the date, 30y, 10y, 2y or 3m
      if(elem.tag.__contains__("NEW_DATE")|elem.tag.__contains__("BC_3MONTH")|elem.tag.__contains__("BC_2YEAR")|elem.tag.__contains__("BC_3YEAR")|elem.tag.__contains__("BC_10YEAR")|elem.tag.__contains__("BC_30YEARDISPLAY")):
        temp_row.append(elem.text)        

    #print(temp_row)
    df_us_treasury_yields.loc[len(df_us_treasury_yields.index)] = temp_row
    #print(elem.tag)
    #print(elem.text)

  # format columns

  df_us_treasury_yields['2Y'] = pd.to_numeric(df_us_treasury_yields['3M'])
  df_us_treasury_yields['2Y'] = pd.to_numeric(df_us_treasury_yields['2Y'])
  df_us_treasury_yields['3Y'] = pd.to_numeric(df_us_treasury_yields['3Y'])
  df_us_treasury_yields['10Y'] = pd.to_numeric(df_us_treasury_yields['10Y'])
  df_us_treasury_yields['30Y'] = pd.to_numeric(df_us_treasury_yields['30Y'])
  df_us_treasury_yields['DATE'] = pd.to_datetime(df_us_treasury_yields['DATE'],format='%Y-%m-%d')

  return df_us_treasury_yields


us_treasury_yields = get_us_treasury_yields()
#TODO: Format Data to match excel spreadsheet

# Get Original Sheet and store it in a dataframe
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

df_updated = combine_df_on_index(df_original, us_treasury_yields, 'DATE')

df_updated = df_updated.drop(columns=['3Y'], axis=1)

# get a list of columns
cols = list(df_updated)
# move the column to head of list using index, pop and insert
cols.insert(0, cols.pop(cols.index('DATE')))
cols.insert(1, cols.pop(cols.index('3M')))
cols.insert(2, cols.pop(cols.index('2Y')))
cols.insert(3, cols.pop(cols.index('10Y')))
cols.insert(4, cols.pop(cols.index('30Y')))

# reorder
df_updated = df_updated[cols]

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

print("Done!")
