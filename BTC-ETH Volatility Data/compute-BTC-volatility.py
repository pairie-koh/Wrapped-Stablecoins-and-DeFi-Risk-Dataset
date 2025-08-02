import pandas as pd
import numpy as np

# Read the BTC data
df = pd.read_csv(r"C:\Users\hello\OneDrive\Desktop\Wrapped Stablecoins\BTC-ETH Data\BTC-USD-binance.csv")

# Convert Date to datetime and sort
df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y')
df = df.sort_values('Date')
df = df.reset_index(drop=True)

# Clean the volume column - remove 'K' and convert to numeric
# Convert K (thousands) to actual numbers
df['Volume'] = df['Vol.'].str.replace('K', '').astype(float) * 1000

# Calculate log returns (following Griffin & Shams JF 2020)
df['log_return'] = np.log(df['Price'] / df['Price'].shift(1))

# Calculate volatility metrics - ONLY what JF papers use
df['volatility_7d'] = df['log_return'].rolling(window=7).std() * np.sqrt(365) * 100
df['volatility_30d'] = df['log_return'].rolling(window=30).std() * np.sqrt(365) * 100

# Add asset identifier
df['Asset'] = 'BTC'

# Keep only necessary columns - now with clean Volume
df_final = df[['Date', 'Asset', 'Price', 'Volume', 'log_return', 
               'volatility_7d', 'volatility_30d']].dropna()

# Display summary
print("BTC Data with Clean Volume:")
print(df_final.head())

# Save
df_final.to_csv('BTC_volatility_JF_style.csv', index=False)