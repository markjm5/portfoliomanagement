from datetime import date
from common import get_oecd_data, write_to_directory

# TODO: Get OECD Data Using API: https://stackoverflow.com/questions/40565871/read-data-from-oecd-api-into-python-and-pandas
# https://stats.oecd.org/restsdmx/sdmx.ashx/GetData/QNA/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LTU+LVA+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+EA19+EU27_2020+G-7+NAFTA+OECDE+G-20+OECD+ARG+BRA+BGR+CHN+IND+IDN+ROU+RUS+SAU+ZAF.B1_GE+P31S14_S15+P3S13+P51+P52_P53+B11+P6+P7.GYSA+GPSA+CTQRGPSA.Q/all?startTime=2019-Q3&endTime=2021-Q3
# https://stats.oecd.org/restsdmx/sdmx.ashx/GetData/QNA/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LTU+LVA+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+EA19+EU27_2020+G-7+NAFTA+OECDE+G-20+OECD+ARG+BRA+BGR+CHN+IND+IDN+ROU+RUS+SAU+ZAF.B1_GE+P31S14_S15+P3S13+P51+P52_P53+B11+P6+P7.GYSA+GPSA+CTQRGPSA.Q/all?startTime=2019-Q3&endTime=2021-Q3

#Get CPI Data from OECD
country = ['AUS','AUT','BEL','CAN','CHL','COL','CRI','CZE','DNK','EST','FIN','FRA','DEU','GRC','HUN','ISL','IRL','ISR','ITA','JPN','KOR','LTU','LVA','LUX','MEX','NLD','NZL','NOR','POL','PRT','SVK','SVN','ESP','SWE','CHE','TUR','GBR','USA','EA19','EU27_2020','G-7','NAFTA','OECDE','G-20','OECD','ARG','BRA','BGR','CHN','IND','IDN','ROU','RUS','SAU','ZAF']
subject = ['B1_GE']
measure = ['GPSA']
frequency = 'Q'
startDate = '1947-Q1'

todays_date = date.today()
endDate = '%s-Q4' % (todays_date.year)

df_global_cpi = get_oecd_data('QNA', [country, subject, measure, [frequency]], {'startTime': startDate, 'endTime': endDate, 'dimensionAtObservation': 'AllDimensions','filename': '008_Global_Core_CPI.xml'})

#Write to a csv file in the correct directory
write_to_directory(df_global_cpi,'003_Lagging_Indicator_World_CPI.csv')

#Get Core CPI Data from OECD
#https://stats.oecd.org/restsdmx/sdmx.ashx/GetData/QNA/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LTU+LVA+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+EA19+EU27_2020+G-7+NAFTA+OECDE+G-20+OECD+ARG+BRA+BGR+CHN+IND+IDN+ROU+RUS+SAU+ZAF.B1_GE+P31S14_S15+P3S13+P51+P52_P53+B11+P6+P7.GYSA+GPSA+CTQRGPSA.Q/all?startTime=2019-Q3&endTime=2021-Q3

country = ['AUS','AUT','BEL','CAN','CHL','COL','CRI','CZE','DNK','EST','FIN','FRA','DEU','GRC','HUN','ISL','IRL','ISR','ITA','JPN','KOR','LTU','LVA','LUX','MEX','NLD','NZL','NOR','POL','PRT','SVK','SVN','ESP','SWE','CHE','TUR','GBR','USA','EA19','EU27_2020','G-7','NAFTA','OECDE','G-20','OECD','ARG','BRA','BGR','CHN','IND','IDN','ROU','RUS','SAU','ZAF']
subject = ['B1_GE']
measure = ['GYSA']
frequency = 'Q'
startDate = '1947-Q1'

todays_date = date.today()
endDate = '%s-Q4' % (todays_date.year)

df_global_core_cpi = get_oecd_data('QNA', [country, subject, measure, [frequency]], {'startTime': startDate, 'endTime': endDate, 'dimensionAtObservation': 'AllDimensions','filename': '008_Global_Core_CPI.xml'})

#Write to a csv file in the correct directory
write_to_directory(df_global_core_cpi,'003_Lagging_Indicator_World_Core_CPI.csv')

print("Done!")
