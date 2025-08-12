import pandas as pd
from datetime import datetime

# Import your CSV file
df = pd.read_csv(r"C:\Users\hello\OneDrive\Desktop\Wrapped Stablecoins\Gas_Prices_Data\arbitrum_gas_fees_arbriscan.csv")

# Remove quotes from column names
df.columns = df.columns.str.replace('"', '')

# Remove quotes from all data entries
for col in df.columns:
    if df[col].dtype == 'object':
        df[col] = df[col].str.replace('"', '')

# Convert date to datetime (after removing quotes)
df['Date(UTC)'] = pd.to_datetime(df['Date(UTC)'])

# Create date range from 01-01-2020 to 08-02-2025
date_range = pd.date_range(start='2020-01-01', end='2025-08-02', freq='D')
new_df = pd.DataFrame({'Date(UTC)': date_range})

# Merge with your data
result = pd.merge(new_df, df, on='Date(UTC)', how='left')

# Save cleaned file without quotes
result.to_csv('cleaned_data.csv', index=False)

print("Done! All quotes removed and dates filled.")