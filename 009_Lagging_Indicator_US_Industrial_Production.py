from common import get_stlouisfed_data, convert_excelsheet_to_dataframe, write_dataframe_to_excel, combine_df_on_index

excel_file_path = '/Trading_Excel_Files/01_Lagging_Coincident_Indicators/009_Lagging_Indicator_US_Industrial_Production.xlsm'
sheet_name = 'database'

df_INDPRO = get_stlouisfed_data('INDPRO')
df_IPB54100S = get_stlouisfed_data('IPB54100S')
df_IPBUSEQ = get_stlouisfed_data('IPBUSEQ')
df_IPCONGD = get_stlouisfed_data('IPCONGD')
df_IPMAN = get_stlouisfed_data('IPMAN')
df_IPMAT = get_stlouisfed_data('IPMAT')
df_IPMINE = get_stlouisfed_data('IPMINE')
df_IPUTIL = get_stlouisfed_data('IPUTIL')
df_TCU = get_stlouisfed_data('TCU')
df_core_ppi = get_stlouisfed_data('WPSFD4131')

#Combine all these data frames into a single data frame based on the DATE field

df = combine_df_on_index(df_INDPRO,df_IPB54100S,"DATE")
df = combine_df_on_index(df_IPBUSEQ,df,"DATE")
df = combine_df_on_index(df_IPCONGD,df,"DATE")
df = combine_df_on_index(df_IPMAN,df,"DATE")
df = combine_df_on_index(df_IPMAT,df,"DATE")
df = combine_df_on_index(df_IPMINE,df,"DATE")
df = combine_df_on_index(df_IPUTIL,df,"DATE")
df = combine_df_on_index(df_TCU,df,"DATE")
df = combine_df_on_index(df_core_ppi,df,"DATE")

df_original = convert_excelsheet_to_dataframe(excel_file_path, sheet_name, True)

df_updated = combine_df_on_index(df_original, df, "DATE")

write_dataframe_to_excel(excel_file_path, sheet_name, df_updated, False, 0)

#Write to a csv file in the correct directory
#write_to_directory(df,'009_Lagging_Indicator_US_Industrial_Production.csv')

print("Done!")
