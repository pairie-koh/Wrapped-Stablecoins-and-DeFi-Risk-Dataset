import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time

class BinanceHourlyHistoricalData:
    """
    Get hourly historical leverage data from Binance (2020-2025)
    """
    
    def __init__(self):
        self.spot_base = "https://api.binance.com/api/v3"
        self.futures_base = "https://fapi.binance.com/fapi/v1"
        self.futures_data = "https://fapi.binance.com/futures/data"
        
        # Define date range
        self.start_date = datetime(2020, 1, 1)
        self.end_date = datetime(2025, 8, 2)
        
    def get_hourly_klines(self, symbol='BTCUSDT', start_date=None, end_date=None):
        """
        Get hourly OHLCV data for entire period
        Binance allows max 1000 candles per request
        """
        if start_date is None:
            start_date = self.start_date
        if end_date is None:
            end_date = self.end_date
            
        url = f"{self.spot_base}/klines"
        
        all_klines = []
        current_start = start_date
        
        while current_start < end_date:
            # Convert to milliseconds
            start_ms = int(current_start.timestamp() * 1000)
            
            params = {
                'symbol': symbol,
                'interval': '1h',  # HOURLY interval
                'startTime': start_ms,
                'limit': 1000  # Max allowed (1000 hours = ~41 days)
            }
            
            print(f"Fetching {symbol} hourly from {current_start.date()}...")
            
            try:
                response = requests.get(url, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if data:
                        for kline in data:
                            # Parse kline data
                            all_klines.append({
                                'timestamp': pd.to_datetime(kline[0], unit='ms'),
                                'open': float(kline[1]),
                                'high': float(kline[2]),
                                'low': float(kline[3]),
                                'close': float(kline[4]),
                                'volume': float(kline[5]),
                                'quote_volume': float(kline[7]),
                                'trades': int(kline[8]),
                                'taker_buy_base': float(kline[9]),
                                'taker_buy_quote': float(kline[10])
                            })
                        
                        # Update start time for next batch
                        last_time = pd.to_datetime(data[-1][0], unit='ms')
                        current_start = last_time + timedelta(hours=1)
                        
                        print(f"  Got {len(data)} hourly candles, up to {last_time}")
                    else:
                        break
                        
                elif response.status_code == 429:
                    print("Rate limited, waiting 60 seconds...")
                    time.sleep(60)
                    continue
                else:
                    print(f"Error: {response.status_code}")
                    break
                    
            except Exception as e:
                print(f"Exception: {e}")
                break
            
            # Small delay to avoid rate limits
            time.sleep(0.5)
        
        if all_klines:
            df = pd.DataFrame(all_klines)
            df = df.drop_duplicates(subset=['timestamp']).sort_values('timestamp')
            print(f"Total: {len(df)} hourly candles from {df['timestamp'].min()} to {df['timestamp'].max()}")
            return df
        
        return None
    
    def get_hourly_open_interest(self, symbol='BTCUSDT'):
        """
        Get hourly open interest data
        """
        url = f"{self.futures_data}/openInterestHist"
        
        all_oi_data = []
        current_date = self.start_date
        
        # Process in chunks of 20 days (480 hours per chunk)
        while current_date < self.end_date:
            end_chunk = min(current_date + timedelta(days=20), self.end_date)
            
            start_ms = int(current_date.timestamp() * 1000)
            end_ms = int(end_chunk.timestamp() * 1000)
            
            params = {
                'symbol': symbol,
                'period': '1h',  # HOURLY
                'startTime': start_ms,
                'endTime': end_ms,
                'limit': 500  # Max 500
            }
            
            print(f"Fetching hourly OI from {current_date.date()} to {end_chunk.date()}...")
            
            try:
                response = requests.get(url, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if data:
                        for item in data:
                            all_oi_data.append({
                                'timestamp': pd.to_datetime(item['timestamp'], unit='ms'),
                                'open_interest': float(item['sumOpenInterest']),
                                'open_interest_usd': float(item['sumOpenInterestValue'])
                            })
                        print(f"  Got {len(data)} hourly OI points")
                    else:
                        print(f"  No data available")
                        
                elif response.status_code == 429:
                    print("Rate limited, waiting...")
                    time.sleep(60)
                    continue
                else:
                    print(f"  Error: {response.status_code}")
                    
            except Exception as e:
                print(f"  Exception: {e}")
            
            current_date = end_chunk
            time.sleep(1)
        
        if all_oi_data:
            df = pd.DataFrame(all_oi_data)
            df = df.drop_duplicates(subset=['timestamp']).sort_values('timestamp')
            print(f"Total hourly OI data: {len(df)} points")
            return df
        
        return None
    
    def get_hourly_funding_rates(self, symbol='BTCUSDT'):
        """
        Get funding rates and align to hourly
        Note: Funding is every 8 hours, we'll forward fill to hourly
        """
        url = f"{self.futures_base}/fundingRate"
        
        all_funding = []
        end_time = int(self.end_date.timestamp() * 1000)
        
        while True:
            params = {
                'symbol': symbol,
                'endTime': end_time,
                'limit': 1000
            }
            
            print(f"Fetching funding rates before {pd.to_datetime(end_time, unit='ms').date()}...")
            
            try:
                response = requests.get(url, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if data:
                        for item in data:
                            funding_time = pd.to_datetime(item['fundingTime'], unit='ms')
                            
                            if funding_time < self.start_date:
                                break
                                
                            all_funding.append({
                                'timestamp': funding_time,
                                'funding_rate': float(item['fundingRate'])
                            })
                        
                        oldest_time = pd.to_datetime(data[-1]['fundingTime'], unit='ms')
                        print(f"  Got {len(data)} funding rates, oldest: {oldest_time.date()}")
                        
                        if oldest_time <= self.start_date or len(data) < 1000:
                            break
                            
                        end_time = int(oldest_time.timestamp() * 1000) - 1
                        
                    else:
                        break
                        
                else:
                    print(f"Error: {response.status_code}")
                    break
                    
            except Exception as e:
                print(f"Exception: {e}")
                break
            
            time.sleep(1)
        
        if all_funding:
            df = pd.DataFrame(all_funding)
            df = df[df['timestamp'] >= self.start_date]
            df = df.drop_duplicates(subset=['timestamp']).sort_values('timestamp')
            
            # Resample to hourly (forward fill)
            df = df.set_index('timestamp')
            hourly_funding = df.resample('1H').ffill()
            hourly_funding = hourly_funding.reset_index()
            
            print(f"Total hourly funding rates: {len(hourly_funding)} points")
            return hourly_funding
        
        return None
    
    def get_hourly_long_short_ratio(self, symbol='BTCUSDT'):
        """
        Get hourly long/short ratio
        """
        url = f"{self.futures_data}/globalLongShortAccountRatio"
        
        all_ls_data = []
        current_date = self.start_date
        
        # Process in chunks
        while current_date < self.end_date:
            end_chunk = min(current_date + timedelta(days=20), self.end_date)
            
            start_ms = int(current_date.timestamp() * 1000)
            end_ms = int(end_chunk.timestamp() * 1000)
            
            params = {
                'symbol': symbol,
                'period': '1h',  # HOURLY
                'startTime': start_ms,
                'endTime': end_ms,
                'limit': 500
            }
            
            print(f"Fetching hourly L/S ratio from {current_date.date()} to {end_chunk.date()}...")
            
            try:
                response = requests.get(url, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if data:
                        for item in data:
                            all_ls_data.append({
                                'timestamp': pd.to_datetime(item['timestamp'], unit='ms'),
                                'longShortRatio': float(item['longShortRatio']),
                                'longAccount': float(item['longAccount']),
                                'shortAccount': float(item['shortAccount'])
                            })
                        print(f"  Got {len(data)} hourly L/S points")
                        
            except Exception as e:
                print(f"  Error: {e}")
            
            current_date = end_chunk
            time.sleep(1)
        
        if all_ls_data:
            df = pd.DataFrame(all_ls_data)
            df = df.drop_duplicates(subset=['timestamp']).sort_values('timestamp')
            print(f"Total hourly L/S data: {len(df)} points")
            return df
        
        return None
    
    def get_hourly_taker_volume(self, symbol='BTCUSDT'):
        """
        Get hourly taker buy/sell volume
        """
        url = f"{self.futures_data}/takerlongshortRatio"
        
        all_taker_data = []
        current_date = self.start_date
        
        while current_date < self.end_date:
            end_chunk = min(current_date + timedelta(days=20), self.end_date)
            
            start_ms = int(current_date.timestamp() * 1000)
            end_ms = int(end_chunk.timestamp() * 1000)
            
            params = {
                'symbol': symbol,
                'period': '1h',  # HOURLY
                'startTime': start_ms,
                'endTime': end_ms,
                'limit': 500
            }
            
            print(f"Fetching hourly taker volume from {current_date.date()} to {end_chunk.date()}...")
            
            try:
                response = requests.get(url, params=params)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if data:
                        for item in data:
                            all_taker_data.append({
                                'timestamp': pd.to_datetime(item['timestamp'], unit='ms'),
                                'buySellRatio': float(item['buySellRatio']),
                                'buyVol': float(item['buyVol']),
                                'sellVol': float(item['sellVol'])
                            })
                        print(f"  Got {len(data)} hourly taker points")
                        
            except Exception as e:
                print(f"  Error: {e}")
            
            current_date = end_chunk
            time.sleep(1)
        
        if all_taker_data:
            df = pd.DataFrame(all_taker_data)
            df = df.drop_duplicates(subset=['timestamp']).sort_values('timestamp')
            print(f"Total hourly taker data: {len(df)} points")
            return df
        
        return None
    
    def combine_hourly_data(self, symbol='BTCUSDT'):
        """
        Combine all hourly data for a symbol
        """
        print(f"\n{'='*60}")
        print(f"Processing {symbol} - HOURLY DATA")
        print('='*60)
        
        # 1. Get hourly prices
        print("\n1. Fetching hourly prices...")
        prices = self.get_hourly_klines(symbol)
        
        if prices is None:
            print(f"Failed to get price data for {symbol}")
            return None
        
        # Use prices as base
        df = prices.set_index('timestamp')
        
        # 2. Get hourly OI
        print("\n2. Fetching hourly Open Interest...")
        oi = self.get_hourly_open_interest(symbol)
        if oi is not None:
            oi = oi.set_index('timestamp')
            df = df.join(oi[['open_interest', 'open_interest_usd']], how='left')
        
        # 3. Get funding rates (resampled to hourly)
        print("\n3. Fetching funding rates...")
        funding = self.get_hourly_funding_rates(symbol)
        if funding is not None:
            funding = funding.set_index('timestamp')
            df = df.join(funding[['funding_rate']], how='left')
        
        # 4. Get L/S ratio
        print("\n4. Fetching hourly Long/Short ratio...")
        ls_ratio = self.get_hourly_long_short_ratio(symbol)
        if ls_ratio is not None:
            ls_ratio = ls_ratio.set_index('timestamp')
            df = df.join(ls_ratio[['longShortRatio', 'longAccount', 'shortAccount']], how='left')
        
        # 5. Get taker volume
        print("\n5. Fetching hourly taker volume...")
        taker = self.get_hourly_taker_volume(symbol)
        if taker is not None:
            taker = taker.set_index('timestamp')
            df = df.join(taker[['buySellRatio', 'buyVol', 'sellVol']], how='left')
        
        # Calculate additional metrics
        df['symbol'] = symbol
        
        # Calculate leverage metrics
        if 'open_interest_usd' in df.columns and 'quote_volume' in df.columns:
            df['oi_to_volume'] = df['open_interest_usd'] / df['quote_volume'].replace(0, np.nan)
            df['oi_change'] = df['open_interest_usd'].diff()
            df['oi_change_pct'] = df['open_interest_usd'].pct_change() * 100
        
        # Reset index
        df = df.reset_index()
        
        print(f"\nCombined {symbol} data: {len(df)} hourly rows")
        print(f"Columns: {', '.join(df.columns)}")
        
        return df
    
    def run_complete_collection(self):
        """
        Collect all hourly data for multiple symbols
        """
        print("\n" + "="*60)
        print("COLLECTING HOURLY HISTORICAL DATA (2020-2025)")
        print("="*60)
        
        symbols = ['BTCUSDT', 'ETHUSDT']  # Add more if needed
        all_data = {}
        
        for symbol in symbols:
            df = self.combine_hourly_data(symbol)
            
            if df is not None:
                all_data[symbol] = df
                
                # Save individual symbol data
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f'{symbol}_hourly_2020_2025_{timestamp}.csv'
                df.to_csv(filename, index=False)
                print(f"\nSaved {symbol} to {filename}")
                
                # Print summary
                print(f"\n{symbol} Summary:")
                print(f"  Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
                print(f"  Total hours: {len(df)}")
                
                if 'open_interest_usd' in df.columns:
                    oi_data = df['open_interest_usd'].dropna()
                    if len(oi_data) > 0:
                        print(f"  OI range: ${oi_data.min():,.0f} to ${oi_data.max():,.0f}")
                        print(f"  Current OI: ${oi_data.iloc[-1]:,.0f}")
                
                if 'funding_rate' in df.columns:
                    funding_data = df['funding_rate'].dropna()
                    if len(funding_data) > 0:
                        print(f"  Avg funding: {funding_data.mean()*100:.4f}%")
            
            # Wait between symbols
            time.sleep(2)
        
        # Combine all symbols
        if all_data:
            combined = pd.concat(all_data.values(), ignore_index=True)
            combined_file = f'all_symbols_hourly_2020_2025_{timestamp}.csv'
            combined.to_csv(combined_file, index=False)
            
            print("\n" + "="*60)
            print("COLLECTION COMPLETE")
            print("="*60)
            print(f"\nTotal combined data: {len(combined)} hourly rows")
            print(f"Saved to: {combined_file}")
            
            # Final statistics
            print("\nFinal Statistics:")
            for symbol in symbols:
                symbol_data = combined[combined['symbol'] == symbol]
                print(f"\n{symbol}:")
                print(f"  Hours of data: {len(symbol_data)}")
                print(f"  Date range: {symbol_data['timestamp'].min()} to {symbol_data['timestamp'].max()}")
                
                # Data completeness
                if 'open_interest_usd' in symbol_data.columns:
                    oi_completeness = (symbol_data['open_interest_usd'].notna().sum() / len(symbol_data)) * 100
                    print(f"  OI data completeness: {oi_completeness:.1f}%")
                
                if 'longShortRatio' in symbol_data.columns:
                    ls_completeness = (symbol_data['longShortRatio'].notna().sum() / len(symbol_data)) * 100
                    print(f"  L/S data completeness: {ls_completeness:.1f}%")

# MAIN EXECUTION
if __name__ == "__main__":
    
    # Initialize
    collector = BinanceHourlyHistoricalData()
    
    # Run complete collection
    collector.run_complete_collection()
    
    print("\n" + "="*60)
    print("HOURLY DATA COLLECTION COMPLETE")
    print("="*60)
    print("\nYou now have:")
    print("1. Hourly OHLCV data (2020-2025)")
    print("2. Hourly Open Interest")
    print("3. Hourly Long/Short Ratios")
    print("4. Hourly Taker Buy/Sell Volume")
    print("5. Funding Rates (8-hour, forward-filled to hourly)")
    print("\nAll saved to CSV files!")