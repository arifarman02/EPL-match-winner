import pandas as pd
import os

# Empty dataframe to hold all match data in order of league position
matches = pd.DataFrame()

# Get a list of all the files
csv_file_list = os.listdir('./EPL_23-24_season')
csv_file_list.sort() # Sort by league position

for csv_file in csv_file_list:
    match = pd.read_csv(csv_file)
    matches = matches.append(match, ignore_index = True)