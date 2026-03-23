import dlt
import pandas as pd
import os
import chardet
import re
from pathlib import Path

def detect_encoding(file_path):
    """Detects the character encoding of a file (UTF-8, ISO-8859-1, etc.)"""
    with open(file_path, 'rb') as f:
        rawdata = f.read(10000)
        result = chardet.detect(rawdata)
        return result['encoding'] or 'utf-8'

def build_table_name(station_name):
    """Build a safe table name for each station/file."""
    cleaned = re.sub(r"[^a-zA-Z0-9_]+", "_", station_name.strip().lower())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    if not cleaned:
        cleaned = "unknown_station"
    if cleaned[0].isdigit():
        cleaned = f"station_{cleaned}"
    return f"climate_{cleaned}_raw"

def stream_rioja_csvs(data_directory):
    """
    Generator that processes each CSV file in the directory.
    """
    path = Path(data_directory)
    
    # Recursively find all .csv files
    for csv_file in path.rglob("*.csv"):
        print(f">>> Processing: {csv_file.name}")
        
        # 1. Get the right encoding
        encoding = detect_encoding(csv_file)
        
        # 2. Read the CSV
        # skiprows=0 as your files seem to have a header on line 1 
        # decimal=',' is crucial for Spanish numeric formats (e.g. 8,1)
        try:
            df = pd.read_csv(
                csv_file, 
                encoding=encoding, 
                sep=',', 
                quotechar='"',
                decimal=',',
                on_bad_lines='warn' # skips and warns about bad lines
            )
            
            # 3. Add metadata for the Data Lake
            df['station_name'] = csv_file.stem
            df['ingested_at'] = pd.Timestamp.now()
            station_table_name = build_table_name(csv_file.stem)
            
            # Yield as a dlt resource
            yield dlt.resource(
                df, 
                table_name=station_table_name,
                write_disposition="append" 
            )
        except Exception as e:
            print(f"!!! Error processing {csv_file.name}: {e}")

def load_data():
    # Define the pipeline for the Data Lake (GCS)
    pipeline = dlt.pipeline(
        pipeline_name="rioja_wine_pipeline",
        destination="filesystem",
        dataset_name="rioja_raw_data",
    )

    # Set the GCS Bucket URL (This can also be set in .dlt/config.toml)
    # Ensure this matches your Terraform bucket name
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = "gs://rioja_wine_lake_raw"

    # Run the pipeline
    # We use 'parquet' format because it's the most efficient for BigQuery later
    data_source = stream_rioja_csvs("rioja_data")
    info = pipeline.run(data_source, loader_file_format="parquet")
    
    print(info)

if __name__ == "__main__":
    load_data()