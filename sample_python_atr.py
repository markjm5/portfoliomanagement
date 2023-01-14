# Import the pandas library
import pandas as pd

# Creates a dataframe called netflix and reads CSV file 'NFLX' which is downloaded from history on Yahoo Finance
netflix_df = pd.read_csv('NFLX.csv')

# Creates a new column in the netflix dataframe called 'H-L' and does the high - low
netflix_df['H-L'] = netflix_df['High'] - netflix_df['Low']

# Creates a new column in the netflix dataframe called 'H-C' which is the absolute value of the high on the current day - close previous day
# the .shift(1) function takes the close from the previous day
netflix_df['H-C'] = abs(netflix_df['High'] - netflix_df['Close'].shift(1))

# Creates a new column in the netflix dataframe called 'L-C' which is the absolute value of the low on the current day - close previous day
netflix_df['L-C'] = abs(netflix_df['Low'] - netflix_df['Close'].shift(1))

# Creates a new column in the netflix dataframe called 'TR' which chooses which is the highest out of the H-L, H-C and L-C values
netflix_df['TR'] = netflix_df[['H-L', 'H-C', 'L-C']].max(axis=1)

# Creates a new column in the netflix datafram called 'ATR' and calculates the ATR
netflix_df['ATR'] = netflix_df['TR'].rolling(14).mean()

# Creates a new dataframe called netflix_sorted_df using the netflix dataframe
# Sorts the dates from newest to oldest, rather than oldest to newest which the Yahoo Finance default
netflix_sorted_df = netflix_df.sort_values(by='Date', ascending = False)

# Creates an excel CSV file with the complete dataframe
netflix_sorted_df.to_csv("netflix_atr.csv")