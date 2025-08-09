import requests
import pandas as pd
from datetime import datetime
import time

# List of chains you need
chains = ['ethereum', 'arbitrum', 'optimism', 'polygon', 'avalanche', 'bsc']

# Store all data
all_chain_data = []

# Get data for each chain
for chain in chains:
    print(f"Fetching data for {chain}...")
    
    # API endpoint for historical chain TVL
    url = f"https://api.llama.fi/v2/historicalChainTvl/{chain}"
    
    try:
        response = requests.get(url)
        data = response.json()
        
        # Add chain name to each record
        for record in data:
            all_chain_data.append({
                'date': datetime.fromtimestamp(record['date']).strftime('%Y-%m-%d'),
                'chain': chain,
                'tvl': record['tvl']
            })
        
        print(f" Got {len(data)} days of data for {chain}")
        
    except Exception as e:
        print(f"✗ Error fetching {chain}: {e}")
    
    # Be nice to their free API
    time.sleep(0.5)

# Convert to DataFrame
df = pd.DataFrame(all_chain_data)

# Filter for your date range (2020-2025)
df['date'] = pd.to_datetime(df['date'])
df = df[(df['date'] >= '2020-01-01') & (df['date'] <= '2025-08-02')]

# Pivot to wide format (optional - makes it easier to use)
df_wide = df.pivot(index='date', columns='chain', values='tvl')
df_wide.reset_index(inplace=True)

# Save both formats
df.to_csv('chain_tvl_historical_long.csv', index=False)
df_wide.to_csv('chain_tvl_historical_wide.csv', index=False)

print(f"\nTotal records: {len(df)}")