import requests
import pandas as pd
from datetime import datetime
import time

# List of chains you need
chains = ['ethereum', 'arbitrum', 'optimism', 'polygon', 'avalanche', 'bsc']

# Store all data
all_chain_data = []

# Get hourly data for each chain
for chain in chains:
    print(f"Fetching HOURLY TVL data for {chain}...")
    
    # API endpoint for hourly chain TVL
    url = f"https://api.llama.fi/v2/historicalChainTvl/{chain}?dataType=hourly"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        # Add chain name to each record
        for record in data:
            all_chain_data.append({
                'datetime': datetime.fromtimestamp(record['date']).strftime('%Y-%m-%d %H:%M:%S'),
                'chain': chain,
                'tvl': record['tvl']
            })
        
        print(f" Got {len(data)} hourly records for {chain}")
        
    except Exception as e:
        print(f"✗ Error fetching {chain}: {e}")
    
    # Be nice to their free API
    time.sleep(0.5)

# Convert to DataFrame
df = pd.DataFrame(all_chain_data)

# Convert datetime column to pandas datetime
df['datetime'] = pd.to_datetime(df['datetime'])

# Filter for your date range (2020–2025)
df = df[(df['datetime'] >= '2020-01-01') & (df['datetime'] <= '2025-08-02')]

# Pivot to wide format
df_wide = df.pivot(index='datetime', columns='chain', values='tvl')
df_wide.reset_index(inplace=True)

# Save both formats
df.to_csv('chain_tvl_hourly_long.csv', index=False)
df_wide.to_csv('chain_tvl_hourly_wide.csv', index=False)

print(f"\nTotal records: {len(df)}")
