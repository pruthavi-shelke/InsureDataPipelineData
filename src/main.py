from ingestion_manager import validate
from preprocessing_engine import preprocess_file

valid_files = validate()
parquet_file_list = preprocess_file(valid_files)
print(parquet_file_list)