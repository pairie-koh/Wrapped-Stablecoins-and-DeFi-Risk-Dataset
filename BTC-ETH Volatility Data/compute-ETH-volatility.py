import pandas as pd
import numpy as np

# Read the ETH data
df = pd.read_csv(r"C:\Users\hello\OneDrive\Desktop\Wrapped Stablecoins\BTC-ETH Price Data\ETH-USD-binance.csv")

# Convert Date to datetime and sort
df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y')
df = df.sort_values('Date')
df = df.reset_index(drop=True)

# Clean volume - handle K, M, and B properly
def convert_volume(vol_str):
    if pd.isna(vol_str) or vol_str == '-':
        return np.nan
    
    vol_str = str(vol_str).strip()
    
    # Check if it contains K, M, or B
    if 'K' in vol_str:
        return float(vol_str.replace('K', '')) * 1000
    elif 'M' in vol_str:
        return float(vol_str.replace('M', '')) * 1000000
    elif 'B' in vol_str:
        return float(vol_str.replace('B', '')) * 1000000000
    else:
        # If no K, M, or B, just return the float
        return float(vol_str)

# Apply the conversion
df['Volume'] = df['Vol.'].apply(convert_volume)

# Calculate log returns
df['log_return'] = np.log(df['Price'] / df['Price'].shift(1))

# Calculate volatility metrics
df['volatility_7d'] = df['log_return'].rolling(window=7).std() * np.sqrt(365) * 100
df['volatility_30d'] = df['log_return'].rolling(window=30).std() * np.sqrt(365) * 100

# Add asset identifier
df['Asset'] = 'ETH'

# Keep only necessary columns
df_final = df[['Date', 'Asset', 'Price', 'Volume', 'log_return', 
               'volatility_7d', 'volatility_30d']].dropna()

# Display summary
print("ETH Volatility Summary:")
print(df_final[['volatility_7d', 'volatility_30d']].describe())

# Save
df_final.to_csv('ETH_volatility_JF_style.csv', index=False)