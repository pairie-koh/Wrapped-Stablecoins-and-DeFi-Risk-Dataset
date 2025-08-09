import requests
import pandas as pd
from datetime import datetime, timezone
import time

def fetch_stablecoin_list():
    """
    Fetch the list of all stablecoins to get their IDs
    """
    url = "https://stablecoins.llama.fi/stablecoins?includePrices=true"
    
    try:
        print("Fetching stablecoin list to get IDs...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Create a mapping of stablecoin names to IDs
        stablecoin_map = {}
        for coin in data['peggedAssets']:
            name = coin.get('name', '').upper()
            symbol = coin.get('symbol', '').upper()
            coin_id = coin.get('id')
            
            # Map common stablecoins
            if 'USDC' in symbol or 'USD COIN' in name.upper():
                stablecoin_map['USDC'] = coin_id
            elif 'USDT' in symbol or 'TETHER' in name.upper():
                stablecoin_map['USDT'] = coin_id
            elif symbol == 'DAI' or 'DAI' in name.upper():
                stablecoin_map['DAI'] = coin_id
            elif 'BUSD' in symbol or 'BINANCE USD' in name.upper():
                stablecoin_map['BUSD'] = coin_id
            elif 'TUSD' in symbol or 'TRUEUSD' in name.upper() or 'TRUE USD' in name.upper():
                stablecoin_map['TUSD'] = coin_id
        
        print(f"Found stablecoin IDs: {stablecoin_map}")
        return stablecoin_map
        
    except Exception as e:
        print(f"Error fetching stablecoin list: {e}")
        # Fallback to known IDs based on common mappings
        print("Using fallback IDs...")
        return {
            'USDT': '1',
            'USDC': '2',
            'BUSD': '3',
            'DAI': '4',
            'TUSD': '5'
        }

def fetch_stablecoin_data(stablecoin_id, name):
    """
    Fetch historical stablecoin data from DeFiLlama API
    
    Args:
        stablecoin_id: Numeric ID for the stablecoin
        name: Display name for the stablecoin
    
    Returns:
        DataFrame with processed stablecoin data
    """
    # Use the stablecoincharts endpoint for historical data
    url = f"https://stablecoins.llama.fi/stablecoincharts/all?stablecoin={stablecoin_id}"
    
    try:
        print(f"Fetching data for {name} (ID: {stablecoin_id}) from: {url}")
        response = requests.get(url, timeout=60)  # Longer timeout for large datasets
        response.raise_for_status()
        
        # Check if response has content
        if not response.content:
            print(f"Empty response for {name}")
            return pd.DataFrame()
        
        data = response.json()
        
        if not data:
            print(f"No data returned for {name}")
            return pd.DataFrame()
        
        # Process the data
        all_data = []
        for entry in data:
            try:
                # Convert Unix timestamp to UTC datetime
                timestamp = int(entry.get('date', 0))
                if timestamp:
                    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                    
                    # Filter for date range (2020-01-01 to 2025-02-08)
                    if dt >= datetime(2020, 1, 1, tzinfo=timezone.utc) and dt <= datetime(2025, 2, 8, 23, 59, 59, tzinfo=timezone.utc):
                        # Extract supply value
                        supply = 0
                        if 'totalCirculating' in entry:
                            circulating = entry['totalCirculating']
                            if isinstance(circulating, dict):
                                supply = circulating.get('peggedUSD', 0)
                            else:
                                supply = circulating
                        elif 'totalCirculatingUSD' in entry:
                            circulating = entry['totalCirculatingUSD']
                            if isinstance(circulating, dict):
                                supply = circulating.get('peggedUSD', 0)
                            else:
                                supply = circulating
                        
                        if supply and supply > 0:
                            all_data.append({
                                'date': dt.strftime('%Y-%m-%d %H:00:00'),
                                'stablecoin': name,
                                'supply': float(supply)
                            })
            except Exception as e:
                continue  # Skip problematic entries
        
        # Create DataFrame
        df = pd.DataFrame(all_data)
        
        if not df.empty:
            # Remove duplicates and sort
            df = df.drop_duplicates(subset=['date', 'stablecoin'])
            df = df.sort_values('date')
        
        print(f"Successfully fetched {len(df)} records for {name}")
        return df
        
    except requests.exceptions.RequestException as e:
        print(f"Request error for {name}: {e}")
        return pd.DataFrame()
    except ValueError as e:
        print(f"JSON parsing error for {name}: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Unexpected error for {name}: {e}")
        return pd.DataFrame()

def main():
    """
    Main function to fetch all stablecoin data and save to CSV
    """
    # First, get the stablecoin IDs
    stablecoin_ids = fetch_stablecoin_list()
    
    # Define stablecoins to fetch
    stablecoins_to_fetch = ['USDC', 'USDT', 'DAI', 'BUSD', 'TUSD']
    
    # Fetch data for each stablecoin
    all_dfs = []
    for coin_name in stablecoins_to_fetch:
        if coin_name in stablecoin_ids:
            coin_id = stablecoin_ids[coin_name]
            df = fetch_stablecoin_data(coin_id, coin_name)
            if not df.empty:
                all_dfs.append(df)
            # Add a small delay to be respectful to the API
            time.sleep(2)
        else:
            print(f"Could not find ID for {coin_name}")
    
    # Combine all dataframes
    if all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True)
        
        # Sort by stablecoin and date
        combined_df = combined_df.sort_values(['stablecoin', 'date'])
        
        # Reset index
        combined_df = combined_df.reset_index(drop=True)
        
        # Save to CSV
        output_file = 'hourly_stablecoin_supply.csv'
        combined_df.to_csv(output_file, index=False)
        print(f"\nData saved to {output_file}")
        
        # Print summary statistics
        print(f"\nTotal records: {len(combined_df)}")
        if len(combined_df) > 0:
            print(f"Date range: {combined_df['date'].min()} to {combined_df['date'].max()}")
            print(f"Stablecoins: {', '.join(combined_df['stablecoin'].unique())}")
        
        # Print first 10 rows
        print("\nFirst 10 rows of the dataset:")
        print(combined_df.head(10).to_string())
        
        # Print sample of each stablecoin
        print("\nSample data for each stablecoin:")
        for coin in combined_df['stablecoin'].unique():
            coin_df = combined_df[combined_df['stablecoin'] == coin]
            print(f"\n{coin} (first 3 records):")
            print(coin_df.head(3).to_string())
        
        # Additional verification
        print("\n" + "="*50)
        print("VERIFICATION")
        print("="*50)
        print(f"CSV file columns: {list(combined_df.columns)}")
        print(f"Data types:\n{combined_df.dtypes}")
        print(f"\nSupply statistics by stablecoin:")
        for coin in combined_df['stablecoin'].unique():
            coin_data = combined_df[combined_df['stablecoin'] == coin]['supply']
            print(f"\n{coin}:")
            print(f"  Records: {len(coin_data)}")
            print(f"  Min supply: ${coin_data.min():,.2f}")
            print(f"  Max supply: ${coin_data.max():,.2f}")
            print(f"  Latest supply: ${coin_data.iloc[-1]:,.2f}")
        
        return combined_df
    else:
        print("No data was successfully fetched")
        return pd.DataFrame()

if __name__ == "__main__":
    # Run the main function
    df = main()
    
    # Additional check for hourly vs daily data
    if not df.empty:
        print("\n" + "="*50)
        print("DATA FREQUENCY CHECK")
        print("="*50)
        # Check if we have hourly data by looking at a sample day
        sample_date = df['date'].iloc[0].split()[0]  # Get the date part
        same_day_records = df[df['date'].str.startswith(sample_date)]
        print(f"Records for {sample_date}: {len(same_day_records)}")
        if len(same_day_records) > 1:
            print("✓ Data appears to be hourly (multiple records per day)")
        else:
            print("⚠ Data appears to be daily (one record per day)")
            print("Note: DeFiLlama may only provide daily granularity for historical data")