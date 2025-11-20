import pandas as pd
from datetime import datetime
# get valid files to process
def preprocess_file(files_to_process):
    # read csv files
    for file in files_to_process:
        df = pd.read_csv(file)
    # Remove Duplicate Rows
        df_removed_duplicate = df.drop_duplicates().copy()
        # print(df_removed_duplicate)
    # Add Audit Columns - ingestion_date ( Current date ), file_date
        df_removed_duplicate['ingestion_date'] = datetime.now()
        date_with_ext = file.split("_")[1]
        file_Date = date_with_ext[:-4]
        df_removed_duplicate["file_date"] = file_Date
        # print(df_removed_duplicate)
    # string columns - trim spaces
        df_removed_duplicate = df_removed_duplicate.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    # save as parquet files
        file_path_split = file.split("/")
        file_name_with_ext = file_path_split[-1]
        file_name = file_name_with_ext.split(".")[0]
        df.to_parquet("../data/preprocessed/"+file_name+".parquet")


