import pandas as pd
from bs4 import BeautifulSoup
from datetime import date
from common import get_stlouisfed_data, get_oecd_data, convert_excelsheet_to_dataframe
from common import combine_df_on_index, write_dataframe_to_excel, get_page

excel_file_path = '/Trading_Excel_Files/01_Lagging_Coincident_Indicators/005_Lagging_Indicator_Job_Market_US_World.xlsm'

#Scrape this table and get latest ADP number
def scrape_world_gdp_table(url):
  #Scrape GDP Table from Trading Economics
  page = get_page(url)

  soup = BeautifulSoup(page.content, 'html.parser')

  table = soup.find('table')
  table_rows = table.find_all('tr', attrs={'class':'an-estimate-row'})
  table_rows_header = table.find_all('tr')[0].find_all('th')
  df = pd.DataFrame()

  index = 0

  for header in table_rows_header:
    if(index == 0):
      df.insert(0,"DATE",[],True)
    else:
      df.insert(index,str(header.text).strip(),[],True)

    index+=1

  #Insert New Row. Format the data to show percentage as float
  for tr in table_rows:
    temp_row = []

    td = tr.find_all('td')
    for obs in td:
        if(str(obs.text).strip() == 'ADP Employment Change'):
            pass #Hidden field, need to pass over
        else:
            text = str(obs.text).strip()
            temp_row.append(text)        

    df.loc[len(df.index)] = temp_row

  return df

##################################
#   Get Data from St Louis Fed   #
##################################

sheet_name = 'Database'

df_CIVPART = get_stlouisfed_data('CIVPART')
df_PAYEMS = get_stlouisfed_data('PAYEMS')
df_UNRATE = get_stlouisfed_data('UNRATE')

df_1 = combine_df_on_index(df_CIVPART,df_PAYEMS,"DATE")
df_1 = combine_df_on_index(df_UNRATE,df_1,"DATE")

df_original_1 = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)
df_updated_1 = combine_df_on_index(df_original_1, df_1,'DATE')

write_dataframe_to_excel(excel_file_path, sheet_name, df_updated_1, False, 0)

sheet_name = 'Database Claims'

df_CCSA = get_stlouisfed_data('CCSA')
df_ICSA = get_stlouisfed_data('ICSA')

#Combine all these data frames into a single data frame
df_2 = combine_df_on_index(df_CCSA,df_ICSA,"DATE")

df_original_2 = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)
df_updated_2 = combine_df_on_index(df_original_2, df_2,'DATE')

write_dataframe_to_excel(excel_file_path, sheet_name, df_updated_2, False, 0)

##########################
#   Get Data from OECD   #
##########################

sheet_name = 'Data World'

country = ['AUS','AUT','BEL','CAN','CHL','CZE','DEU','DNK','ESP','EST','FIN','FRA','GBR','GRC','HUN','IRL','ISL','ISR','ITA','JPN','KOR','LUX','LVA','MEX','NLD','NOR','OECD','POL','PRT','SVK','SVN','SWE','TUR','USA','EA19','G-7','CHE']

subject = ['LRHUTTTT']
measure = ['STSA']
frequency = 'M'
startDate = '1955-Q1'

todays_date = date.today()
endDate = '%s-Q4' % (todays_date.year)

df_unemployed_world = get_oecd_data('STLABOUR', [country, subject, measure, [frequency]], {'startTime': startDate, 'endTime': endDate, 'dimensionAtObservation': 'AllDimensions','filename': '005_Job_Market_World.xml'})

df_unemployed_world = df_unemployed_world.drop(columns='MTH', axis=1)

df_original_unemployed_world = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

# Check for difference between original and new lists
#print(util_check_diff_list(df_unemployed_world.columns.tolist(), df_original_unemployed_world.columns.tolist()))

df_updated_unemployed_world = combine_df_on_index(df_original_unemployed_world, df_unemployed_world,'DATE')

# get a list of columns
cols = list(df_updated_unemployed_world)
# move the column to head of list using index, pop and insert
cols.insert(0, cols.pop(cols.index('DATE')))

# reorder
df_updated_unemployed_world = df_updated_unemployed_world[cols]

# format date
df_updated_unemployed_world['DATE'] = pd.to_datetime(df_updated_unemployed_world['DATE'],format='%d/%m/%Y')

write_dataframe_to_excel(excel_file_path, sheet_name, df_updated_unemployed_world, False, 0)

#Write to a csv file in the correct directory
#write_to_directory(df_unemployed_world,'005_Lagging_Indicator_Job_Market_World.csv')

#######################################################
# Get Employment ADP Data from Trading Economics Site #
#######################################################

sheet_name = 'Database ADP'

df_adp = scrape_world_gdp_table("https://tradingeconomics.com/united-states/adp-employment-change")

#Drop unnecessary columns
df_adp = df_adp.drop(columns='GMT', axis=1)
df_adp = df_adp.drop(columns='Reference', axis=1)
df_adp = df_adp.drop(columns='Previous', axis=1)
df_adp = df_adp.drop(columns='Consensus', axis=1)
df_adp = df_adp.drop(columns='TEForecast', axis=1)

#Rename column
df_adp = df_adp.rename(columns={"Actual": "ADP"})

#Fix datatypes of df_world_gdp
df_adp['DATE'] = pd.to_datetime(df_adp['DATE'],format='%Y-%m-%d')
df_adp['ADP'] = df_adp['ADP'].str.slice(0,3)  #Remove the K from the end of the string, and convert it to an int

df_original_adp = convert_excelsheet_to_dataframe(excel_file_path, sheet_name)
last_row_original_adp = df_original_adp.iloc[-1]

# Loop through each row in df_adp and if it doesn't exist in df_original_adp add it to the end. 
for index, row in df_adp.iterrows():
  if(not row['DATE'] in df_original_adp['DATE'].values):
    if(not pd.isnull(row['ADP'])):
      df_original_adp = df_original_adp.append(row, ignore_index = True)

df_original_adp['ADP'] = pd.to_numeric(df_original_adp['ADP'])

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_original_adp, False,0)

print("Done!")
