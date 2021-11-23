import requests
import sys
import os.path
import csv
import pandas as pd
import xml.etree.ElementTree as ET

def get_stlouisfed_data(series_code):
  url = "https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=8067a107f45ff78491c1e3117245a0a3&file_type=json" % (series_code,)

  resp = requests.get(url=url)
  json = resp.json() 
  
  df = pd.DataFrame(columns=["DATE",series_code])

  for i in range(len(json["observations"])):
    df = df.append({"DATE": json["observations"][i]["date"], series_code: json["observations"][i]["value"]}, ignore_index=True)

  print("Retrieved Data for Series %s" % (series_code,))

  return df

def get_oecd_data(dataset, dimensions, params):

  dim_args = ['+'.join(d) for d in dimensions]
  dim_str = '.'.join(dim_args)
  
  date_range = dimensions[3][0]
  if(date_range == 'Q'):
    date_range = 'QTR'
  elif(date_range == 'M'):
    date_range = 'MTH'

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
    df.insert(0,date_range,[],True)
    df.insert(1,"DATE",[],True)
    index = 2
    for series in root.findall('./sdmx:DataSet/sdmx:Series',ns):
      current_country = ""
      for value in series.findall('./sdmx:SeriesKey/sdmx:Value',ns): 
        if(value.get('concept')) == 'LOCATION':
          current_country = value.get('value')
          
          df.insert(index,value.get('value'),[],True)
      index+=1

    #Add Dates  TODO: This needs to account for both Quarterly and Monthly data.
    date_range_list = []
    date_list = []
    year_start, qtr_start =  params['startTime'].split('-Q')
    year_end, qtr_end =  params['endTime'].split('-Q')

    if(date_range == 'QTR'):

      for x in range(int(year_start), int(year_end)+1):
        for y in range(1,5):

          qtr_string = "%s-Q%s" % (x, y)
          date_range_list.append(qtr_string)
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

    else:
      #From year_start to year_end, calculate all the months. Populate date_range_list and date_list
      date_range_list = pd.date_range('%s-01-01' % (year_start),'%s-01-01' % (year_end), freq='MS').strftime("%Y-%m").tolist()
      date_list = pd.date_range('%s-01-01' % (year_start),'%s-01-01' % (year_end), freq='MS').strftime("1/%-m/%Y").tolist()

    df[date_range] = date_range_list
    df['DATE'] = date_list

    #Add observations for all countries
    #TODO: Does not work for monthly jobless data

    for series in root.findall('./sdmx:DataSet/sdmx:Series',ns):
      for value in series.findall('./sdmx:SeriesKey/sdmx:Value',ns): 
        if(value.get('concept')) == 'LOCATION':
          current_country = value.get('value')

          observations = series.findall('sdmx:Obs',ns)

          for observation in observations:
            #TODO: Get qtr, date and observation. Add to column in chronological order for all countries

            obs_qtr = observation.findall('sdmx:Time',ns)[0].text
            obs_row = df.index[df[date_range] == obs_qtr].tolist()
            obs_value = observation.findall('sdmx:ObsValue',ns)[0].get('value')

            #match based on quarter and country, then add the observation value
            df.loc[obs_row, current_country] = round(float(obs_value),9)        
    return df

  except Exception as e:
    
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    print(exc_type, fname, exc_tb.tb_lineno)


def write_to_directory(df,filename):
    #Write to a csv file in the correct directory
    userhome = os.path.expanduser('~')
    file_name = os.path.join(userhome, 'Desktop', 'Trading_Excel_Files', 'Database',filename)
    df.to_csv(file_name, index=False)

