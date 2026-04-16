import os
import pandas as pd
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from autotrader_collector import fetch_autotrader_listings

# Load env vars
load_dotenv()

BRONZE_PATH = Path("data/bronze")
BRONZE_PATH.mkdir(parents=True, exist_ok=True)

def ingest_autotrader(make="BMW", fuel_type="Petrol", pages=1):
    """
    Fetches and saves AutoTrader listings directly as Parquet files to the Local Bronze layer.
    """
    all_listings = []
    for p in range(1, pages + 1):
        print(f"DEBUG: Fetching {make} ({fuel_type}) - Page {p}")
        listings = fetch_autotrader_listings(
            make=make, fuel_type_filter=fuel_type, pages=p
        )
        if listings:
            all_listings.extend(listings)

    if not all_listings:
        print(f"WARN: No data found for {make} ({fuel_type}).")
        return

    # Convert to DataFrame
    df = pd.DataFrame(all_listings)
    
    # Basic Cleaning: Clean price column for numeric processing in Silver
    if 'price' in df.columns:
        df['price'] = df['price'].astype(str).str.replace('£', '').str.replace(',', '').replace('None', '0')
        df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)

    # Add metadata
    df['ingestion_timestamp'] = datetime.now()
    df['brand'] = make
    df['fuel_type_raw'] = fuel_type

    # Save to Bronze in Parquet format
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"{make.lower().replace('-', '_')}_{fuel_type.lower()}_{timestamp_str}.parquet"
    save_path = BRONZE_PATH / file_name
    
    df.to_parquet(save_path, index=False)
    print(f"OK: {make} {fuel_type} saved to {save_path} ({len(all_listings)} records).")

if __name__ == "__main__":
    print("START: ULEZ Live Data Engine Started...")
    print("INFO: Mode: Local Data Lake (Bronze Ingestion)")

    # Ensure data directory exists
    if not BRONZE_PATH.exists():
        BRONZE_PATH.mkdir(parents=True)

    brands = ["BMW", "Mercedes-Benz", "Audi", "Volkswagen"]
    for brand in brands:
        for fuel in ["Petrol", "Diesel"]:
            ingest_autotrader(make=brand, fuel_type=fuel, pages=1)

    print("\nDONE: Local Bronze update complete.")
