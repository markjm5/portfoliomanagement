import requests
import os.path
import csv
import pandas as pd

def get_data(series_code):
  url = "https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=8067a107f45ff78491c1e3117245a0a3&file_type=json" % (series_code,)

  resp = requests.get(url=url)
  json = resp.json() 
  
  df = pd.DataFrame(columns=["DATE",series_code])

  for i in range(len(json["observations"])):
    df = df.append({"DATE": json["observations"][i]["date"], series_code: json["observations"][i]["value"]}, ignore_index=True)

  print("Retrieved Data for Series %s" % (series_code,))

  return df

df_DPCCRV1Q225SBEA = get_data('DPCCRV1Q225SBEA')
df_EXPGSC1 = get_data('EXPGSC1')
df_GCEC1 = get_data('GCEC1')
df_GDPC1 = get_data('GDPC1')
df_GDPCTPI = get_data('GDPCTPI')
df_GPDIC1 = get_data('GPDIC1')
df_IMPGSC1 = get_data('IMPGSC1')
df_JCXFE = get_data('JCXFE')
df_PCECC96 = get_data('PCECC96')

#Combine all these data frames into a single data frame based on the DATE field

df = pd.merge(df_DPCCRV1Q225SBEA,df_EXPGSC1,"right")
df = pd.merge(df,df_GCEC1,"left")
df = pd.merge(df,df_GDPC1,"left")
df = pd.merge(df,df_GDPCTPI,"left")
df = pd.merge(df,df_GPDIC1,"left")
df = pd.merge(df,df_IMPGSC1,"left")
df = pd.merge(df,df_JCXFE,"left")
df = pd.merge(df,df_PCECC96,"left")

#Write to a csv file in the correct directory
userhome = os.path.expanduser('~')
file_name = os.path.join(userhome, 'Desktop', 'Trading_Excel_Files', 'Database','002_Lagging_Indicator_US_GDP.csv')
df.to_csv(file_name, index=False)

print("Done!")
