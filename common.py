import requests
import os.path
import csv
import pandas as pd

def get_stlouisfed_data(series_code):
  url = "https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=8067a107f45ff78491c1e3117245a0a3&file_type=json" % (series_code,)

  resp = requests.get(url=url)
  json = resp.json() 
  
  df = pd.DataFrame(columns=["DATE",series_code])

  for i in range(len(json["observations"])):
    df = df.append({"DATE": json["observations"][i]["date"], series_code: json["observations"][i]["value"]}, ignore_index=True)

  print("Retrieved Data for Series %s" % (series_code,))

  return df

def write_to_directory(df,filename):
    #Write to a csv file in the correct directory
    userhome = os.path.expanduser('~')
    file_name = os.path.join(userhome, 'Desktop', 'Trading_Excel_Files', 'Database',filename)
    df.to_csv(file_name, index=False)

