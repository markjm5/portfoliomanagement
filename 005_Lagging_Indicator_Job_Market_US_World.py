import requests
import os.path
import csv
import pandas as pd
from common import get_stlouisfed_data, write_to_directory

df_CIVPART = get_stlouisfed_data('CIVPART')
df_PAYEMS = get_stlouisfed_data('PAYEMS')
df_UNRATE = get_stlouisfed_data('UNRATE')

df_CCSA = get_stlouisfed_data('CCSA')
df_ICSA = get_stlouisfed_data('ICSA')

#Combine all these data frames into a single data frame

df_1 = pd.merge(df_CIVPART,df_PAYEMS,"right")
df_1 = pd.merge(df_1,df_UNRATE,"left")

df_2 = pd.merge(df_CCSA,df_ICSA,"right")

df = pd.concat([df_1,df_2],axis=1)

#Write to a csv file in the correct directory
write_to_directory(df,'005_Lagging_Indicator_Job_Market_US_World.csv')

print("Done!")
