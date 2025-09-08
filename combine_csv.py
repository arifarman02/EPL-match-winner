import pandas as pd
import os

# Empty dataframe to hold all match data in order of league position
matches = pd.DataFrame()

# Get a list of all the files
csv_file_list = os.listdir('./EPL_23-24_season')
csv_file_list.sort() # Sort by league position

# An empty list to concat into pandas dataframe
all_dfs = []

# Loop through each file in the csv list and read each into a pandas dataframe and append them into all_dsf list
for csv_file in csv_file_list:
    full_path = os.path.join('./EPL_23-24_season', csv_file) # CSV file is in a different directory
    match = pd.read_csv(full_path, encoding='latin-1')
    all_dfs.append(match)

# Get the second season data
second_csv_file_list = os.listdir('./EPL_24-25_season')
second_csv_file_list.sort()

for csv_file in second_csv_file_list:
    full_path = os.path.join('./EPL_24-25_season', csv_file)
    match = pd.read_csv(full_path, encoding='latin-1')
    all_dfs.append(match)

# Combine all the dataframes into one
matches = pd.concat(all_dfs, ignore_index=True)

# Save the comined dataframe into a csv file
matches.to_csv('matches.csv', index=False)