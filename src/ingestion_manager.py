import json
import glob
import os 
import pandas as pd

# get all files from config/control_file
# read all files, one by one 
# validate files in data/ingestion
# add valid file in list
# return list
def validate():
    # get all files from config/control_file
    file_lists = glob.glob('../config/control_files/*.json')
    valid_file_list = []

    # read all files, one by one
    for file in file_lists:
        with open(file, 'r') as f:
            data = json.load(f)
            # validate files in data/ingestion
                # does file exists in folder data/ingestion
            
            file_path = "../data/ingestion/" + data["file_name"]
            if os.path.exists(file_path):
                df = pd.read_csv(file_path)
                if len(df) == data["expected_rows"]:
                    expected_col = data["expected_columns"]
                    df_col =  list(df.columns)
                    if df_col == expected_col:
                        valid_file_list.append(file_path)
    return valid_file_list



