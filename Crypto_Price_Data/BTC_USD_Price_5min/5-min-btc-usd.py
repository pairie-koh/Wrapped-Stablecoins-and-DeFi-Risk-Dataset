import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import time
import os

class CoinbaseDataFetcher:
    """
    A class to fetch historical candle data from Coinbase Advanced Trade API.
    Updated to use the new v3 API endpoints.
    """
    
    def __init__(self):
        self.base_url = "https://api.coinbase.com/api/v3/brokerage"
        self.product_id = "BTC-USD"
        
        # Granularity mapping from seconds to API format
        self.granularity_map = {
            60: "ONE_MINUTE",
            300: "FIVE_MINUTE",
            900: "FIFTEEN_MINUTE",
            3600: "ONE_HOUR",
            21600: "SIX_HOUR",
            86400: "ONE_DAY"
        }
        
    def fetch_candles(self, start_time=None, end_time=None, granularity=300, limit=300):
        """
        Fetch historical candles data from Coinbase Advanced Trade API.
        """
        
        # Convert granularity to API format
        if granularity not in self.granularity_map:
            print(f"Invalid granularity: {granularity}. Valid values: {list(self.granularity_map.keys())}")
            return None
            
        granularity_str = self.granularity_map[granularity]
        
        # Construct the endpoint URL
        endpoint = f"{self.base_url}/market/products/{self.product_id}/candles"
        
        # Set up parameters
        params = {
            "granularity": granularity_str,
            "limit": min(limit, 350)  # API max is 350
        }
        
        # Convert datetime to Unix timestamp if needed
        if start_time:
            if isinstance(start_time, datetime):
                params["start"] = str(int(start_time.timestamp()))
            else:
                params["start"] = start_time
                
        if end_time:
            if isinstance(end_time, datetime):
                params["end"] = str(int(end_time.timestamp()))
            else:
                params["end"] = end_time
        
        # Validate range if both provided
        if start_time and end_time:
            start_ts = int(params["start"])
            end_ts = int(params["end"])
            
            # Calculate number of candles in the range
            time_diff = end_ts - start_ts
            num_candles = time_diff / granularity
            
            if num_candles > 300:
                # Adjust end time to stay within limit
                params["end"] = str(start_ts + (300 * granularity))
        
        try:
            # Make the API request
            headers = {
                'Content-Type': 'application/json',
                'User-Agent': 'Python/CoinbaseDataFetcher'
            }
            
            response = requests.get(endpoint, params=params, headers=headers)
            response.raise_for_status()
            
            # Parse the response
            data = response.json()
            
            # Extract candles from response
            if 'candles' in data:
                candles = data['candles']
            else:
                return None
            
            if not candles:
                return None
            
            # Convert to DataFrame
            df = pd.DataFrame(candles)
            
            # Rename columns to match expected format
            column_mapping = {
                'start': 'timestamp',
                'low': 'low',
                'high': 'high',
                'open': 'open',
                'close': 'close',
                'volume': 'volume'
            }
            df = df.rename(columns=column_mapping)
            
            # Convert timestamp to datetime (from Unix timestamp string)
            df['timestamp'] = pd.to_datetime(df['timestamp'].astype(int), unit='s')
            
            # Convert price columns to float
            for col in ['low', 'high', 'open', 'close', 'volume']:
                if col in df.columns:
                    df[col] = df[col].astype(float)
            
            # Sort by timestamp
            df = df.sort_values('timestamp')
            
            # Reset index
            df = df.reset_index(drop=True)
            
            return df
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error: {e}")
            return None
    
    def fetch_complete_historical_data(self, start_date, end_date, granularity=300):
        """
        Fetch complete historical data for a large date range.
        Optimized for very long time periods.
        """
        
        # Convert string dates to datetime if needed
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
        
        # Calculate total time range
        total_seconds = (end_date - start_date).total_seconds()
        total_candles = total_seconds / granularity
        
        # Calculate batch size (max 300 candles per request)
        batch_seconds = 300 * granularity
        batch_timedelta = timedelta(seconds=batch_seconds)
        
        # Calculate number of requests needed
        num_requests = int(total_candles / 300) + (1 if total_candles % 300 > 0 else 0)
        
        print(f"="*60)
        print(f"FETCHING COMPLETE HISTORICAL DATA: {self.product_id}")
        print(f"="*60)
        print(f"Date range: {start_date.date()} to {end_date.date()}")
        print(f"Total days: {(end_date - start_date).days}")
        print(f"Granularity: {granularity/60} minutes")
        print(f"Total expected candles: ~{int(total_candles):,}")
        print(f"Estimated requests needed: {num_requests:,}")
        print(f"Estimated time: {(num_requests * 0.5 / 60):.1f} minutes (with rate limiting)")
        print(f"="*60)
        
        all_data = []
        current_start = start_date
        request_count = 0
        failed_requests = []
        
        # Main checkpoint file
        main_checkpoint = f"btc_usd_checkpoint_complete.csv"
        
        # Yearly checkpoint files for backup
        yearly_data = {}
        
        # Load existing checkpoint if it exists
        if os.path.exists(main_checkpoint):
            print(f"\nFound existing checkpoint file: {main_checkpoint}")
            existing_df = pd.read_csv(main_checkpoint)
            existing_df['timestamp'] = pd.to_datetime(existing_df['timestamp'])
            all_data.append(existing_df)
            
            # Update start point to continue from last timestamp
            last_timestamp = existing_df['timestamp'].max()
            current_start = last_timestamp + timedelta(seconds=granularity)
            print(f"Resuming from: {current_start}")
            print(f"Already have {len(existing_df):,} candles")
        
        print("\nStarting data fetch...")
        print("Press Ctrl+C to pause (progress will be saved)")
        
        while current_start < end_date:
            request_count += 1
            
            # Calculate end time for this batch
            current_end = min(current_start + batch_timedelta, end_date)
            
            # Progress indicator
            progress = ((current_start - start_date).total_seconds() / total_seconds) * 100
            
            # Less verbose output - update every 10 requests
            if request_count % 10 == 1:
                print(f"\nProgress: {progress:.1f}% | Request {request_count}/{num_requests}")
                print(f"Current: {current_start.strftime('%Y-%m-%d %H:%M')}")
            
            # Fetch data for this period
            retry_count = 0
            max_retries = 3
            df = None
            
            while retry_count < max_retries and df is None:
                df = self.fetch_candles(current_start, current_end, granularity)
                
                if df is None:
                    retry_count += 1
                    if retry_count < max_retries:
                        time.sleep(2)
                    else:
                        failed_requests.append((current_start, current_end))
            
            if df is not None and not df.empty:
                all_data.append(df)
                
                # Save checkpoint every 100 requests (about 8.3 hours of data)
                if request_count % 100 == 0:
                    print(f"  Saving checkpoint at request {request_count}...")
                    temp_df = pd.concat(all_data, ignore_index=True)
                    temp_df = temp_df.drop_duplicates(subset=['timestamp'])
                    temp_df = temp_df.sort_values('timestamp')
                    temp_df.to_csv(main_checkpoint, index=False)
                    print(f"  Checkpoint saved: {len(temp_df):,} total candles")
                    
                    # Also save yearly backups
                    for year in temp_df['timestamp'].dt.year.unique():
                        year_df = temp_df[temp_df['timestamp'].dt.year == year]
                        year_file = f"btc_usd_5min_{year}_backup.csv"
                        year_df.to_csv(year_file, index=False)
            
            # Move to next batch
            current_start = current_end
            
            # Rate limiting
            time.sleep(0.5)
        
        # Combine all data
        if all_data:
            print(f"\n{'='*60}")
            print("FINALIZING DATA...")
            
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # Remove duplicates
            original_len = len(combined_df)
            combined_df = combined_df.drop_duplicates(subset=['timestamp'])
            if original_len != len(combined_df):
                print(f"Removed {original_len - len(combined_df)} duplicate candles")
            
            # Sort by timestamp
            combined_df = combined_df.sort_values('timestamp')
            
            # Reset index
            combined_df = combined_df.reset_index(drop=True)
            
            print(f"Total candles fetched: {len(combined_df):,}")
            
            # Report failed requests
            if failed_requests:
                print(f"\nWarning: {len(failed_requests)} requests failed")
                fail_file = "failed_requests.txt"
                with open(fail_file, 'w') as f:
                    for start, end in failed_requests:
                        f.write(f"{start},{end}\n")
                print(f"Failed requests saved to {fail_file}")
            
            # Clean up checkpoint
            if os.path.exists(main_checkpoint):
                os.remove(main_checkpoint)
            
            return combined_df
        else:
            print("No data was fetched")
            return None
    
    def save_complete_dataset(self, df):
        """
        Save the complete dataset with multiple output formats
        """
        if df is None or df.empty:
            print("No data to save")
            return
        
        print("\n" + "="*60)
        print("SAVING COMPLETE DATASET")
        print("="*60)
        
        # Main complete file
        main_file = "btc_usd_5min_complete_20191101_20250802.csv"
        df.to_csv(main_file, index=False)
        print(f"Main file saved: {main_file}")
        print(f"File size: {os.path.getsize(main_file) / (1024*1024):.2f} MB")
        
        # Save yearly files for easier handling
        print("\nSaving yearly files...")
        for year in sorted(df['timestamp'].dt.year.unique()):
            year_df = df[df['timestamp'].dt.year == year]
            year_file = f"btc_usd_5min_{year}.csv"
            year_df.to_csv(year_file, index=False)
            print(f"  {year}: {len(year_df):,} candles -> {year_file}")
        
        # Summary statistics
        print("\n" + "="*60)
        print("DATASET SUMMARY")
        print("="*60)
        print(f"Total candles: {len(df):,}")
        print(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
        print(f"\nPrice Journey:")
        print(f"  Nov 2019 Start: ${df['close'].iloc[0]:,.2f}")
        print(f"  Aug 2025 End: ${df['close'].iloc[-1]:,.2f}")
        print(f"  All-time High: ${df['high'].max():,.2f}")
        print(f"  All-time Low: ${df['low'].min():,.2f}")
        
        # Data completeness
        time_range = (df['timestamp'].max() - df['timestamp'].min()).total_seconds()
        expected_candles = time_range / 300
        completeness = (len(df) / expected_candles) * 100
        print(f"\nData completeness: {completeness:.1f}%")


# Main execution
if __name__ == "__main__":
    # Create fetcher instance
    fetcher = CoinbaseDataFetcher()
    
    # Define date range
    start_date = datetime(2019, 11, 1, 0, 0, 0)    # Nov 1, 2019
    end_date = datetime(2025, 8, 2, 23, 59, 59)     # Aug 2, 2025
    
    print("="*60)
    print("BTC-USD COMPLETE HISTORICAL DATA FETCHER")
    print("="*60)
    print(f"Fetching ALL 5-minute candles from Nov 2019 to Aug 2025")
    print(f"This is approximately 5.75 years of data!")
    print("")
    print("Expected time: 30-45 minutes")
    print("The script will save checkpoints every 100 requests")
    print("You can safely interrupt and resume anytime")
    print("="*60)
    
    # Auto-start after 5 seconds
    print("\nStarting in 5 seconds...")
    print("(Press Ctrl+C to cancel)")
    time.sleep(5)
    
    # Fetch the complete historical data
    df = fetcher.fetch_complete_historical_data(
        start_date=start_date,
        end_date=end_date,
        granularity=300  # 5 minutes
    )
    
    if df is not None:
        # Save everything
        fetcher.save_complete_dataset(df)
        
        print("\n" + "="*60)
        print("SUCCESS! COMPLETE DATASET READY")
        print("="*60)
        print(f"You now have {len(df):,} candles of BTC-USD data")
        print("Files created:")
        print("  - btc_usd_5min_complete_20191101_20250802.csv (main file)")
        print("  - Yearly files from 2019 to 2025")
        print("\nThis dataset is ready for your volatility analysis!")
    else:
        print("\nFetch failed. Check error messages above.")
        print("If you have a checkpoint file, just run the script again to resume.")