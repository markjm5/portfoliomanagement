from common import get_us_treasury_yields, convert_excelsheet_to_dataframe, write_dataframe_to_excel
from common import combine_df_on_index

excel_file_path = '/Trading_Excel_Files/02_Interest_Rates_FX/013_Yield_Curve.xlsm'
sheet_name = 'Database'

us_treasury_yields = get_us_treasury_yields()
#TODO: Format Data to match excel spreadsheet

# Get Original Sheet and store it in a dataframe
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

df_updated = combine_df_on_index(df_original, us_treasury_yields, 'DATE')

df_updated = df_updated.drop(columns=['3Y'], axis=1)

# get a list of columns
cols = list(df_updated)
# move the column to head of list using index, pop and insert
cols.insert(0, cols.pop(cols.index('DATE')))
cols.insert(1, cols.pop(cols.index('2Y')))
cols.insert(2, cols.pop(cols.index('10Y')))
cols.insert(3, cols.pop(cols.index('30Y')))

# reorder
df_updated = df_updated[cols]

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

print("Done!")
