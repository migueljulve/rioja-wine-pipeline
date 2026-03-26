import dlt
import pandas as pd
import os
import chardet
from pathlib import Path
from datetime import datetime

# 1. english language re-naming for the df columns
CLIMATE_COLUMNS = [
    "date", 
    "temp_avg", "temp_max", "temp_min",
    "rel_hum_avg", "rel_hum_max", "rel_hum_min",
    "solar_rad_acc",
    "wind_speed_ms_avg", "wind_speed_ms_max",
    "wind_speed_kmh_avg", "wind_speed_kmh_max",
    "wind_dir_avg", "wind_dir_max",
    "precip_acc",
    "eto_penman_monteith",
    "soil_temp_avg", "soil_temp_max", "soil_temp_min",
    "leaf_wetness_1", "leaf_wetness_2"
]

def detect_encoding(file_path):
    """Detects file encoding for Spanish exports."""
    with open(file_path, 'rb') as f:
        rawdata = f.read(10000)
        result = chardet.detect(rawdata)
        return result['encoding'] or 'utf-8'

# --- RESOURCE 1: ALL 23 STATIONS/tables INTO ONE TABLE ---
@dlt.resource(
    name="weather_stations_raw", # This becomes the table name in BigQuery
    write_disposition="replace", 
    primary_key=("date", "station_name")
)
def climate_stations_resource(data_directory):
    """
    Processes the 23 climate station files and unifies them.
    All files are yielded into the SAME resource (table).
    """
    path = Path(data_directory) / "climate_stations"
    
    for csv_file in path.glob("*.csv"):
        print(f">>> [BATCH] Unifying Station: {csv_file.name}")
        encoding = detect_encoding(csv_file)
        
        try:
            # skiprows=3 drops the Metadata, Parameter, and Function rows
            df = pd.read_csv(
                csv_file, 
                encoding=encoding, 
                skiprows=3,          
                names=CLIMATE_COLUMNS, 
                decimal=',',        
                na_values='-',      
                on_bad_lines='warn'
            )
            
            # Type Conversion
            df['date'] = pd.to_datetime(df['date'], dayfirst=True, errors='coerce')
            
            # Metadata to distinguish stations and track ingestion
            df['station_name'] = csv_file.stem
            df['source_file'] = csv_file.name
            df['ingested_at'] = datetime.utcnow()
            
            # Yield the dataframe as a list of dictionaries
            # Since we removed 'mark.with_table_name', it stays in 'weather_stations_raw'
            yield df.to_dict(orient='records')
            
        except Exception as e:
            print(f"!!! Error processing {csv_file.name}: {e}")

# --- RESOURCE 2: THE HISTORY/METADATA TABLE ---
@dlt.resource(
    name="rioja_wine_history", # This becomes the second table
    write_disposition="replace"
)
def history_resource(data_directory):
    """Processes the history/metadata file."""
    history_file = Path(data_directory) / "rioja_history.csv"
    if history_file.exists():
        print(f">>> [BATCH] Ingesting History Metadata")
        df = pd.read_csv(history_file, encoding=detect_encoding(history_file))
        yield df

def load_data():
    # Initialize dlt pipeline
    pipeline = dlt.pipeline(
        pipeline_name="rioja_unified_pipeline",
        destination="filesystem", # Landing in GCS
        dataset_name="rioja_raw_data",
    )
    
    # Bucket URL
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = "gs://rioja_wine_lake_raw"

    # Define the two unified sources
    data_source = [
        climate_stations_resource("rioja_data"),
        history_resource("rioja_data")
    ]
    
    # Run and convert to Parquet
    info = pipeline.run(data_source, loader_file_format="parquet")
    print(info)

if __name__ == "__main__":
    load_data()