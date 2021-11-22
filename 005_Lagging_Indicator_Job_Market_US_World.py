import requests
import os.path
import csv
import pandas as pd
from datetime import date
from common import get_stlouisfed_data, get_oecd_data, write_to_directory

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
write_to_directory(df,'005_Lagging_Indicator_Job_Market_US.csv')


#TODO: Get Correct Data from OECD
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
write_to_directory(df_QoQ,'005_Lagging_Indicator_Job_Market_World.csv')


print("Done!")
