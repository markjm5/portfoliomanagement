import requests
import os.path
import csv
import pandas as pd
from common import get_stlouisfed_data, write_to_directory

df_PCEPI = get_stlouisfed_data('PCEPI')
df_PCEPILFE = get_stlouisfed_data('PCEPILFE')

#Combine all these data frames into a single data frame based on the DATE field

df = pd.merge(df_PCEPI,df_PCEPILFE,"right")

#Write to a csv file in the correct directory
write_to_directory(df,'006_Lagging_Indicator_Personal_Consumption_Expenditures.csv')

print("Done!")