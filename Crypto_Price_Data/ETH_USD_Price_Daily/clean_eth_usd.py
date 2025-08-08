import pandas as pd

# Read the CSV file with your specific path
df = pd.read_csv(r"C:\Users\hello\OneDrive\Desktop\Wrapped Stablecoins\Crypto_Price_Data\ETH_USD_Price_Daily\eth_usd_daily_with_volatility.csv")

# Convert date column to datetime
df['date'] = pd.to_datetime(df['date'])

# Filter for date range 2020-01-01 to 2025-08-02
start_date = '2020-01-01'
end_date = '2025-08-02'

df_filtered = df[(df['date'] >= start_date) & (df['date'] <= end_date)]

# Sort by date to ensure proper ordering
df_filtered = df_filtered.sort_values('date')

# Reset index
df_filtered = df_filtered.reset_index(drop=True)

# Save to same directory with new name
output_path = r"C:\Users\hello\OneDrive\Desktop\Wrapped Stablecoins\Crypto_Price_Data\BTC_USD_Price_Daily\btc_usd_daily_2020_2025.csv"
df_filtered.to_csv(output_path, index=False)

print(f"Original dataset: {len(df)} rows")
print(f"Filtered dataset: {len(df_filtered)} rows")
print(f"Date range: {df_filtered['date'].min()} to {df_filtered['date'].max()}")
print(f"Saved to: {output_path}")