from ingestion_manager import validate
from preprocessing_engine import preprocess_file

valid_files = validate()
preprocess_file(valid_files)