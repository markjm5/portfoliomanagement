from common import get_stlouisfed_data, convert_excelsheet_to_dataframe, write_dataframe_to_excel
from common import combine_df_on_index

excel_file_path = '/Trading_Excel_Files/01_Lagging_Coincident_Indicators/011_Lagging_Indicator_Durable_Goods.xlsm'
sheet_name = 'Database'

df_DGORDER = get_stlouisfed_data('DGORDER')
df_ADXTNO = get_stlouisfed_data('ADXTNO')
df_NEWORDER = get_stlouisfed_data('NEWORDER')
df_AMTUNO = get_stlouisfed_data('AMTUNO')
df_A36SNO = get_stlouisfed_data('A36SNO')
df_A32SNO = get_stlouisfed_data('A32SNO')
df_A33SNO = get_stlouisfed_data('A33SNO')
df_A34SNO = get_stlouisfed_data('A34SNO')
df_A31SNO = get_stlouisfed_data('A31SNO')
df_A35SNO = get_stlouisfed_data('A35SNO')

#Combine all these data frames into a single data frame based on the DATE field

df = combine_df_on_index(df_DGORDER,df_ADXTNO,"DATE")
df = combine_df_on_index(df_NEWORDER,df,"DATE")
df = combine_df_on_index(df_AMTUNO,df,"DATE")
df = combine_df_on_index(df_A36SNO,df,"DATE")
df = combine_df_on_index(df_A32SNO,df,"DATE")
df = combine_df_on_index(df_A33SNO,df,"DATE")
df = combine_df_on_index(df_A34SNO,df,"DATE")
df = combine_df_on_index(df_A31SNO,df,"DATE")
df = combine_df_on_index(df_A35SNO,df,"DATE")

# Get Original Sheet and store it in a dataframe
df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

df_updated = combine_df_on_index(df_original, df, 'DATE')

# Write the updated df back to the excel sheet
write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

print("Done!")