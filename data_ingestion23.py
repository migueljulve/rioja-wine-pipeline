import dlt
import pandas as pd
import os
import chardet
from pathlib import Path

# Explicit 21-column mapping based on Rioja Parameter/Function matrix
CLIMATE_COLUMNS = [
    "date", 
    "t_med", "t_max", "t_min",           # Temperature (Med/Max/Min)
    "hr_med", "hr_max", "hr_min",         # Humidity (Med/Max/Min)
    "rg_ac",                              # Accumulated Radiation
    "vv_ms_med", "vv_ms_max",             # Wind Speed m/s (Med/Max)
    "vv_kmh_med", "vv_kmh_max",           # Wind Speed km/h (Med/Max)
    "dv_deg_med", "dv_deg_max",           # Wind Direction (Med/Max)
    "p_ac",                               # Accumulated Precipitation
    "eto_calc",                           # Reference Evapotranspiration
    "ts_med", "ts_max", "ts_min",         # Soil Temperature (Med/Max/Min)
    "humh_1_ac", "humh_2_ac"              # Leaf Wetness (Acumulado)
]

def detect_encoding(file_path):
    """Detects file encoding (usually Latin-1 for Spanish exports)"""
    with open(file_path, 'rb') as f:
        rawdata = f.read(10000)
        result = chardet.detect(rawdata)
        return result['encoding'] or 'utf-8'

@dlt.resource(write_disposition="replace")
def climate_stations_resource(data_directory):
    """
    Processes the 23 climate station files.
    Skips the matrix headers and applies explicit naming.
    """
    path = Path(data_directory) / "climate_stations"
    
    for csv_file in path.glob("*.csv"):
        print(f">>> [BATCH] Ingesting Station: {csv_file.name}")
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
            
            # Metadata for Data Lake tracking
            df['station_name'] = csv_file.stem
            df['ingested_at'] = pd.Timestamp.now()
            
            # Create a clean table name 
            clean_name = csv_file.stem.lower().replace(' ', '_').replace('.', '')
            table_name = f"station_{clean_name}"
            
            # THE FIX: Yield the dataframe and mark it with a specific table name.
            # This allows one resource function to feed multiple tables.
            yield dlt.mark.with_table_name(df, table_name)
            
        except Exception as e:
            print(f"!!! Error processing {csv_file.name}: {e}")

@dlt.resource(table_name="rioja_wine_history", write_disposition="replace")
def history_resource(data_directory):
    """
    Processes the history/metadata file. 
    """
    history_file = Path(data_directory) / "rioja_history.csv"
    if history_file.exists():
        print(f">>> [BATCH] Ingesting History Metadata")
        df = pd.read_csv(history_file, encoding=detect_encoding(history_file))
        yield df

def load_data():
    # Initialize dlt pipeline
    pipeline = dlt.pipeline(
        pipeline_name="rioja_wine_pipeline",
        destination="filesystem", # Landing in GCS
        dataset_name="rioja_raw_data",
    )
    
    # Bucket URL
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = "gs://rioja_wine_lake_raw"

    # Define sources
    data_source = [
        climate_stations_resource("rioja_data"),
        history_resource("rioja_data")
    ]
    
    # Run and convert to Parquet for BigQuery efficiency
    info = pipeline.run(data_source, loader_file_format="parquet")
    print(info)

if __name__ == "__main__":
    load_data()