import re
import pandas as pd
from bs4 import BeautifulSoup
from common import get_data_fred, get_sp500_monthly_prices, convert_excelsheet_to_dataframe, write_dataframe_to_excel
from common import combine_df_on_index, get_ism_manufacturing_content, scrape_ism_manufacturing_headline_index, _util_check_diff_list

excel_file_path = '/Trading_Excel_Files/03_Leading_Indicators/016_Leading_Indicator_US_ISM_Manufacturing.xlsm'

def scrape_manufacturing_new_orders_production():

    ism_date, ism_month, page = get_ism_manufacturing_content()

    soup = BeautifulSoup(page.content, 'html.parser')

    #get all paragraphs
    paras = soup.find_all("p", attrs={'class': None})

    para_manufacturing = "" 
    para_new_orders = ""
    para_production = ""

    for para in paras:
        #Get the specific paragraph
        if('manufacturing industries reporting growth in %s' % (ism_month) in para.text):
            para_manufacturing = para.text

        if('growth in new orders' in para.text and '%s' % (ism_month) in para.text):
            para_new_orders = para.text

        if('growth in production' in para.text and '%s' % (ism_month) in para.text):
            para_production = para.text

    return para_manufacturing, para_new_orders, para_production, ism_date, ism_month

def scrape_industry_comments():

    ism_date, ism_month, page = get_ism_manufacturing_content()

    soup = BeautifulSoup(page.content, 'html.parser')

    #Get all html tables on the page
    lis = soup.find_all('li')   
    arr_comments = []

    #The regex pattern for industry comments
    pattern = re.compile("“[()’A-Za-z,&;\s\.0-9\-]*”\s(\[)[A-Za-z,&;\s]*(\])")

    for li in lis:
        matched = pattern.match(li.text)
        if(matched):
            arr_comments.append(li.text)

    return arr_comments

def return_df_comments(arr_comments):
    ism_date, ism_month, page = get_ism_manufacturing_content()

    df_comments = pd.DataFrame(columns=['Date','Sector','Comments'])

    #TODO: Use regex to extract comment and industry name. Regex Cheat Sheet: https://www.rexegg.com/regex-quickstart.html
    pattern_comment = re.compile(r'“[()’A-Za-z,&;\s\.0-9\-]*”')
    pattern_industry = re.compile(r'((?<=\[)[A-Za-z,&;\s]*(?<!\]))')

    for comment in arr_comments:
        matches_comment = re.search(pattern_comment,comment).group(0)
        matches_industry = re.search(pattern_industry,comment).group(0)
        df_comments = df_comments.append({'Date': ism_date, 'Sector': matches_industry, 'Comments': matches_comment}, ignore_index=True)

    return df_comments

def extract_rankings(industry_str):

    ism_date, ism_month, page = get_ism_manufacturing_content()

    #Use regex (https://pythex.org/) to get substring that contains order of industries. It should return 2 matches - for increase and decrease   
    pattern_select = re.compile(r'((?<=following order:\s)[A-Za-z,&;\s]*.|(?<=are:\s)[A-Za-z,&;\s]*.|(?<=are\s)[A-Za-z,&;\s]*.)')
    matches = pattern_select.finditer(industry_str)
    match_arr = []
    pattern_remove = r'and|\.'
    for match in matches:
        new_str = re.sub(pattern_remove, '',match.group(0))
        match_arr.append(new_str)

    #put increase and decrease items into arrays
    increase_arr = match_arr[0].split(';')
    try:
        decrease_arr = []
        decrease_arr = match_arr[1].split(';')        
    except IndexError as e:
        #There must only be one industry reporting decrease, so extract that one.
        pattern_select_decrease = re.compile(r'(only\sindustry[A-Za-z,&;\s]*)')        
        match = pattern_select_decrease.search(industry_str)

        if(match):
            pattern_remove = r'only\sindustry[A-Za-z,&;\s]*is\s'
            new_str = re.sub(pattern_remove, '',match.group(0))
            if(new_str):
                decrease_arr.append(new_str)

    df_rankings = pd.DataFrame()

    #Add Rankings columns to df
    ranking = len(increase_arr)
    index = 0
    for industry in increase_arr:
        df_rankings[industry.lstrip()] = [ranking - index]      
        index += 1

    ranking = len(decrease_arr)
    index = 0
    for industry in decrease_arr:
        df_rankings[industry.lstrip()] = [0 - (ranking - index)]      
        index += 1

    if(len(df_rankings.columns) < 18):
        df_columns_18_industries = ['Machinery','Computer & Electronic Products','Paper Products','Apparel, Leather & Allied Products','Printing & Related Support Activities',
                            'Primary Metals','Nonmetallic Mineral Products','Petroleum & Coal Products','Plastics & Rubber Products','Miscellaneous Manufacturing',
                            'Food, Beverage & Tobacco Products','Furniture & Related Products','Transportation Equipment','Chemical Products','Fabricated Metal Products',
                            'Electrical Equipment, Appliances & Components','Textile Mills','Wood Products']

        #Find out what columns are missing
        missing_columns = _util_check_diff_list(df_columns_18_industries,df_rankings.columns)
        
        #Add missing columns to df_ranking with zero as the rank number
        for col in missing_columns:
            df_rankings[col] = [0]

    #Add DATE column to df
    df_rankings["DATE"] = [ism_date]

    # Reorder Columns
    # get a list of columns
    cols = list(df_rankings)
    # move the column to head of list using index, pop and insert
    cols.insert(0, cols.pop(cols.index('DATE')))
    cols.insert(1, cols.pop(cols.index('Apparel, Leather & Allied Products')))
    cols.insert(2, cols.pop(cols.index('Machinery')))
    cols.insert(3, cols.pop(cols.index('Paper Products')))
    cols.insert(4, cols.pop(cols.index('Computer & Electronic Products')))
    cols.insert(5, cols.pop(cols.index('Petroleum & Coal Products')))
    cols.insert(6, cols.pop(cols.index('Primary Metals')))
    cols.insert(7, cols.pop(cols.index('Printing & Related Support Activities')))
    cols.insert(8, cols.pop(cols.index('Furniture & Related Products')))
    cols.insert(9, cols.pop(cols.index('Transportation Equipment')))
    cols.insert(10, cols.pop(cols.index('Chemical Products')))
    cols.insert(11, cols.pop(cols.index('Food, Beverage & Tobacco Products')))
    cols.insert(12, cols.pop(cols.index('Miscellaneous Manufacturing')))
    cols.insert(13, cols.pop(cols.index('Electrical Equipment, Appliances & Components')))
    cols.insert(14, cols.pop(cols.index('Plastics & Rubber Products')))
    cols.insert(15, cols.pop(cols.index('Fabricated Metal Products')))
    cols.insert(16, cols.pop(cols.index('Wood Products')))
    cols.insert(17, cols.pop(cols.index('Textile Mills')))
    cols.insert(18, cols.pop(cols.index('Nonmetallic Mineral Products')))

    # reorder
    df_rankings = df_rankings[cols]

    return df_rankings


#df_at_a_glance, df_new_orders, df_production, para_manufacturing, para_new_orders, para_production = scrape_ism_manufacturing_index(ism_date)
para_manufacturing, para_new_orders, para_production, ism_date, ism_month = scrape_manufacturing_new_orders_production()

##################################
# Get Manufacturing ISM Rankings #
##################################

sheet_name = 'DB Manufacturing ISM'

#Get rankings
df_manufacturing_rankings = extract_rankings(para_manufacturing)

# Load original data from excel file into original df
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

#Combine new data with original data
df_updated = combine_df_on_index(df_original, df_manufacturing_rankings, 'DATE')

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

###########################
# Get New Orders Rankings #
###########################

sheet_name = 'DB New Orders'

#Get rankings
df_new_orders_rankings = extract_rankings(para_new_orders)

# Load original data from excel file into original df
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

#Combine new data with original data
df_updated = combine_df_on_index(df_original, df_new_orders_rankings, 'DATE')

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

###########################
# Get Production Rankings #
###########################

sheet_name = 'DB Production'

#Get rankings
df_production_rankings = extract_rankings(para_production)

# Load original data from excel file into original df
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

#Combine new data with original data
df_updated = combine_df_on_index(df_original, df_production_rankings, 'DATE')

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

#################################################
# Update Details Tab Using ISM Headline Numbers #
#################################################

sheet_name = 'DB Details'

df_ism_headline_index = scrape_ism_manufacturing_headline_index(ism_date, ism_month)

#################################
# Get US GDP from St Louis FRED #
#################################

#Get US GDP
#df_GDPC1 = get_gdp_fred('GDPC1')
df_GDPC1 = get_data_fred('GDPC1', 'GDPC1', 'Q')

###########################################
# Get S&P500 Monthly Close Prices from YF #
###########################################

df_SP500 = get_sp500_monthly_prices()

# Combine df_LEI, df_GDPC1, df_SP500 and df_UMCSI into original df
df = combine_df_on_index(df_ism_headline_index, df_GDPC1, 'DATE')
df = combine_df_on_index(df_SP500, df, 'DATE')

# Load original data from excel file into original df
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

#Combine new data with original data
df_updated = combine_df_on_index(df_original, df, 'DATE')

# get a list of columns
cols = list(df_updated)
# move the column to head of list
cols.insert(0, cols.pop(cols.index('DATE')))
cols.insert(1, cols.pop(cols.index('NEW_ORDERS')))
cols.insert(2, cols.pop(cols.index('IMPORTS')))
cols.insert(3, cols.pop(cols.index('BACKLOG_OF_ORDERS')))
cols.insert(4, cols.pop(cols.index('PRICES')))
cols.insert(5, cols.pop(cols.index('PRODUCTION')))
cols.insert(6, cols.pop(cols.index('CUSTOMERS_INVENTORIES')))
cols.insert(7, cols.pop(cols.index('INVENTORIES')))
cols.insert(8, cols.pop(cols.index('DELIVERIES')))
cols.insert(9, cols.pop(cols.index('EMPLOYMENT')))
cols.insert(10, cols.pop(cols.index('EXPORTS')))
cols.insert(11, cols.pop(cols.index('ISM')))
cols.insert(12, cols.pop(cols.index('SP500')))
cols.insert(13, cols.pop(cols.index('GDPC1')))
cols.insert(14, cols.pop(cols.index('GDPQoQ')))
cols.insert(15, cols.pop(cols.index('GDPYoY')))
cols.insert(16, cols.pop(cols.index('GDPQoQ_ANNUALIZED')))

# reorder
df_updated = df_updated[cols]

#Fill in blank values with previous
df_updated['GDPYoY'].fillna(method='ffill', inplace=True)
df_updated['GDPQoQ'].fillna(method='ffill', inplace=True)
df_updated['GDPQoQ_ANNUALIZED'].fillna(method='ffill', inplace=True)

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

############################
# Get Respondents Comments #
############################

sheet_name = 'Industry Comments'

# Scrape 'What Respondents Are Saying' comments:
arr_comments = scrape_industry_comments()
df_comments = return_df_comments(arr_comments)

# Load original data from excel file into original df
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, False)

df_updated = df_original.append(df_comments, ignore_index=True)

# Order by Sector in Ascending Order, then by Date in Decending Order
df_updated = df_updated.sort_values(by=['Sector','Date'], ascending=(True,False))
df_updated = df_updated.drop_duplicates(subset=['Sector','Date'])
df_updated = df_updated.reset_index(drop=True)

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 1)

print("Done!")
