import requests
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import os

class CoinbaseETHDailyDataFetcher:
    """
    A class to fetch historical DAILY candle data for ETH-USD from Coinbase Advanced Trade API.
    """
    
    def __init__(self):
        self.base_url = "https://api.coinbase.com/api/v3/brokerage"
        self.product_id = "ETH-USD"  # Changed to ETH-USD
        
        # Granularity mapping from seconds to API format
        self.granularity_map = {
            60: "ONE_MINUTE",
            300: "FIVE_MINUTE",
            900: "FIFTEEN_MINUTE",
            3600: "ONE_HOUR",
            21600: "SIX_HOUR",
            86400: "ONE_DAY"  # Daily = 86400 seconds
        }
        
    def fetch_candles(self, start_time=None, end_time=None, granularity=86400, limit=300):
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
    
    def fetch_daily_historical_data(self, start_date, end_date):
        """
        Fetch complete DAILY historical data for the date range.
        Much faster than 5-minute data since we need fewer requests.
        """
        
        # Convert string dates to datetime if needed
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
        
        # Calculate total days
        total_days = (end_date - start_date).days
        
        # With daily granularity, we can fetch 300 days per request
        days_per_request = 300
        num_requests = (total_days // days_per_request) + (1 if total_days % days_per_request > 0 else 0)
        
        print(f"="*60)
        print(f"FETCHING DAILY HISTORICAL DATA: {self.product_id}")
        print(f"="*60)
        print(f"Date range: {start_date.date()} to {end_date.date()}")
        print(f"Total days: {total_days}")
        print(f"Granularity: DAILY (1 candle per day)")
        print(f"Total expected candles: ~{total_days}")
        print(f"Estimated requests needed: {num_requests}")
        print(f"Estimated time: {(num_requests * 0.5 / 60):.1f} minutes")
        print(f"="*60)
        
        all_data = []
        current_start = start_date
        request_count = 0
        failed_requests = []
        
        print("\nStarting data fetch...")
        
        while current_start < end_date:
            request_count += 1
            
            # Calculate end time for this batch (300 days)
            current_end = min(current_start + timedelta(days=days_per_request), end_date)
            
            # Progress indicator
            progress = ((current_start - start_date).days / total_days) * 100
            
            print(f"\nRequest {request_count}/{num_requests} ({progress:.1f}% complete)")
            print(f"  Fetching: {current_start.strftime('%Y-%m-%d')} to {current_end.strftime('%Y-%m-%d')}")
            
            # Fetch data for this period
            retry_count = 0
            max_retries = 3
            df = None
            
            while retry_count < max_retries and df is None:
                df = self.fetch_candles(current_start, current_end, granularity=86400)  # 86400 seconds = 1 day
                
                if df is None:
                    retry_count += 1
                    if retry_count < max_retries:
                        print(f"  Retry {retry_count}/{max_retries} after 2 seconds...")
                        time.sleep(2)
                    else:
                        print(f"  Failed after {max_retries} attempts")
                        failed_requests.append((current_start, current_end))
            
            if df is not None and not df.empty:
                all_data.append(df)
                print(f"  Retrieved {len(df)} daily candles")
            else:
                print(f"  No data retrieved")
            
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
            
            # Add date column for easier analysis
            combined_df['date'] = combined_df['timestamp'].dt.date
            
            # Reset index
            combined_df = combined_df.reset_index(drop=True)
            
            print(f"Total daily candles fetched: {len(combined_df)}")
            
            # Report failed requests
            if failed_requests:
                print(f"\nWarning: {len(failed_requests)} requests failed")
            
            return combined_df
        else:
            print("No data was fetched")
            return None
    
    def save_daily_dataset(self, df):
        """
        Save the daily ETH dataset with analysis-ready features
        """
        if df is None or df.empty:
            print("No data to save")
            return
        
        print("\n" + "="*60)
        print("SAVING DAILY ETH DATASET")
        print("="*60)
        
        # Add useful columns for analysis
        df['returns'] = df['close'].pct_change()
        df['log_returns'] = (df['close'] / df['close'].shift(1)).apply(lambda x: None if pd.isna(x) else np.log(x))
        df['range'] = df['high'] - df['low']
        df['range_pct'] = (df['high'] - df['low']) / df['close'] * 100
        df['intraday_volatility'] = df['range_pct']  # Proxy for daily volatility
        
        # Main complete file
        main_file = "eth_usd_daily_complete_20191101_20250802.csv"
        df.to_csv(main_file, index=False)
        print(f"Main file saved: {main_file}")
        print(f"File size: {os.path.getsize(main_file) / 1024:.2f} KB")
        
        # Save yearly files
        print("\nSaving yearly files...")
        for year in sorted(df['timestamp'].dt.year.unique()):
            year_df = df[df['timestamp'].dt.year == year]
            year_file = f"eth_usd_daily_{year}.csv"
            year_df.to_csv(year_file, index=False)
            print(f"  {year}: {len(year_df)} days -> {year_file}")
        
        # Summary statistics
        print("\n" + "="*60)
        print("DAILY ETH DATASET SUMMARY")
        print("="*60)
        print(f"Total trading days: {len(df)}")
        print(f"Date range: {df['timestamp'].min().date()} to {df['timestamp'].max().date()}")
        
        print(f"\nPrice Statistics:")
        print(f"  Nov 2019 Start: ${df['close'].iloc[0]:,.2f}")
        print(f"  Aug 2025 End: ${df['close'].iloc[-1]:,.2f}")
        print(f"  All-time High: ${df['high'].max():,.2f}")
        print(f"  All-time Low: ${df['low'].min():,.2f}")
        print(f"  Average Close: ${df['close'].mean():,.2f}")
        
        print(f"\nVolatility Metrics:")
        print(f"  Daily Returns Std Dev: {df['returns'].std()*100:.2f}%")
        print(f"  Annualized Volatility: {df['returns'].std()*np.sqrt(365)*100:.2f}%")
        print(f"  Max Daily Gain: {df['returns'].max()*100:.2f}%")
        print(f"  Max Daily Loss: {df['returns'].min()*100:.2f}%")
        
        print(f"\nKey ETH Events in Dataset:")
        # Find biggest moves
        df_sorted_gains = df.nlargest(5, 'returns')[['date', 'close', 'returns']]
        df_sorted_losses = df.nsmallest(5, 'returns')[['date', 'close', 'returns']]
        
        print("\nTop 5 Daily Gains:")
        for _, row in df_sorted_gains.iterrows():
            print(f"  {row['date']}: +{row['returns']*100:.2f}% (${row['close']:,.2f})")
        
        print("\nTop 5 Daily Losses:")
        for _, row in df_sorted_losses.iterrows():
            print(f"  {row['date']}: {row['returns']*100:.2f}% (${row['close']:,.2f})")
        
        # Calculate rolling volatility for the paper
        print("\nCalculating rolling 30-day volatility...")
        df['volatility_30d'] = df['returns'].rolling(window=30).std() * np.sqrt(365)
        df['volatility_30d_pct'] = df['volatility_30d'] * 100
        
        # Save enhanced version with volatility
        enhanced_file = "eth_usd_daily_with_volatility.csv"
        df.to_csv(enhanced_file, index=False)
        print(f"\nEnhanced file with volatility saved: {enhanced_file}")
        
        # ETH-specific analysis for DeFi
        print("\n" + "="*60)
        print("ETH-SPECIFIC DEFI ANALYSIS")
        print("="*60)
        
        # Identify key DeFi periods
        defi_summer_start = pd.Timestamp('2020-06-01')
        defi_summer_end = pd.Timestamp('2020-10-01')
        defi_period = df[(df['timestamp'] >= defi_summer_start) & (df['timestamp'] <= defi_summer_end)]
        
        if not defi_period.empty:
            print(f"\nDeFi Summer 2020 (Jun-Oct):")
            print(f"  Start Price: ${defi_period['close'].iloc[0]:,.2f}")
            print(f"  End Price: ${defi_period['close'].iloc[-1]:,.2f}")
            print(f"  Return: {((defi_period['close'].iloc[-1] / defi_period['close'].iloc[0]) - 1) * 100:.2f}%")
            print(f"  Volatility: {defi_period['returns'].std() * np.sqrt(365) * 100:.2f}%")
        
        # March 2023 USDC depeg period (key for your paper)
        usdc_depeg_start = pd.Timestamp('2023-03-01')
        usdc_depeg_end = pd.Timestamp('2023-03-31')
        depeg_period = df[(df['timestamp'] >= usdc_depeg_start) & (df['timestamp'] <= usdc_depeg_end)]
        
        if not depeg_period.empty:
            print(f"\nMarch 2023 USDC Depeg Period:")
            print(f"  ETH Start: ${depeg_period['close'].iloc[0]:,.2f}")
            print(f"  ETH End: ${depeg_period['close'].iloc[-1]:,.2f}")
            print(f"  Max Volatility: {depeg_period['returns'].std() * np.sqrt(365) * 100:.2f}%")
            print(f"  Largest Daily Move: {depeg_period['returns'].abs().max() * 100:.2f}%")
        
        # Compare ETH vs BTC if BTC data exists
        btc_file = "btc_usd_daily_complete_20191101_20250802.csv"
        if os.path.exists(btc_file):
            print("\n" + "="*60)
            print("ETH vs BTC COMPARISON")
            print("="*60)
            
            btc_df = pd.read_csv(btc_file)
            btc_df['timestamp'] = pd.to_datetime(btc_df['timestamp'])
            
            # Merge on timestamp
            merged = pd.merge(df[['timestamp', 'close', 'returns']], 
                            btc_df[['timestamp', 'close', 'returns']], 
                            on='timestamp', 
                            suffixes=('_eth', '_btc'))
            
            # Calculate correlation
            correlation = merged['returns_eth'].corr(merged['returns_btc'])
            print(f"ETH-BTC Correlation: {correlation:.3f}")
            
            # Calculate beta (ETH sensitivity to BTC)
            cov_matrix = merged[['returns_eth', 'returns_btc']].cov()
            beta = cov_matrix.loc['returns_eth', 'returns_btc'] / merged['returns_btc'].var()
            print(f"ETH Beta to BTC: {beta:.3f}")
            
            # Volatility comparison
            eth_vol = merged['returns_eth'].std() * np.sqrt(365) * 100
            btc_vol = merged['returns_btc'].std() * np.sqrt(365) * 100
            print(f"ETH Annualized Vol: {eth_vol:.2f}%")
            print(f"BTC Annualized Vol: {btc_vol:.2f}%")
            print(f"ETH/BTC Vol Ratio: {eth_vol/btc_vol:.2f}x")
            
            # Save combined dataset
            combined_file = "eth_btc_daily_combined.csv"
            merged.to_csv(combined_file, index=False)
            print(f"\nCombined ETH-BTC file saved: {combined_file}")
        
        return df


# Main execution
if __name__ == "__main__":
    # Create fetcher instance
    fetcher = CoinbaseETHDailyDataFetcher()
    
    # Define date range
    start_date = datetime(2019, 11, 1, 0, 0, 0)    # Nov 1, 2019
    end_date = datetime(2025, 8, 2, 23, 59, 59)     # Aug 2, 2025
    
    print("="*60)
    print("ETH-USD DAILY DATA FETCHER")
    print("="*60)
    print(f"Fetching DAILY ETH candles from Nov 2019 to Aug 2025")
    print("")
    print("ETH daily data is crucial for your DeFi analysis:")
    print("  - Most wrapped stablecoins operate on Ethereum")
    print("  - ETH volatility drives gas fees (wrapping costs)")
    print("  - DeFi liquidations correlate with ETH price moves")
    print("  - The '23% ETH volatility spike' in your paper")
    print("")
    print("This will be MUCH faster than 5-minute data!")
    print("Expected time: 2-3 minutes")
    print("="*60)
    
    # Auto-start after 3 seconds
    print("\nStarting in 3 seconds...")
    print("(Press Ctrl+C to cancel)")
    time.sleep(3)

    # Fetch the daily historical data
    df = fetcher.fetch_daily_historical_data(
        start_date=start_date,
        end_date=end_date
    )
    
    if df is not None:
        # Save and analyze
        df = fetcher.save_daily_dataset(df)
        
        print("\n" + "="*60)
        print("SUCCESS! DAILY ETH DATASET READY")
        print("="*60)
        print(f"You now have {len(df)} days of ETH-USD data")
        print("\nFiles created:")
        print("  - eth_usd_daily_complete_20191101_20250802.csv")
        print("  - eth_usd_daily_with_volatility.csv (includes 30-day rolling vol)")
        print("  - eth_btc_daily_combined.csv (if BTC data exists)")
        print("  - Yearly files (2019-2025)")
        print("\nThis ETH daily data is perfect for:")
        print("  1. Testing DeFi volatility regimes")
        print("  2. Analyzing ETH-BTC spillovers")
        print("  3. Studying gas fee impacts on wrapped tokens")
        print("  4. Examining the March 2023 USDC depeg event")
    else:
        print("\nFetch failed. Check error messages above.")