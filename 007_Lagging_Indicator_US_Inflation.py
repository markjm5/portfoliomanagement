import requests
import os.path
import csv
import pandas as pd
from common import get_stlouisfed_data, write_to_directory


df_CPIAUCSL = get_stlouisfed_data('CPIAUCSL')
df_CPIENGSL = get_stlouisfed_data('CPIENGSL')
df_CPIFABSL = get_stlouisfed_data('CPIFABSL')
df_CPILFESL = get_stlouisfed_data('CPILFESL')

#Combine all these data frames into a single data frame based on the DATE field

df = pd.merge(df_CPIAUCSL,df_CPIENGSL,"left")
df = pd.merge(df,df_CPIFABSL,"left")
df = pd.merge(df,df_CPILFESL,"left")

#Write to a csv file in the correct directory
write_to_directory(df,'007_Lagging_Indicator_US_Inflation.csv')

print("Done!")