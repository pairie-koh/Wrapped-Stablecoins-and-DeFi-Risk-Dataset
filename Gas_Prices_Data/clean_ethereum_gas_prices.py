import pandas as pd
import numpy as np

# Load your data
file_path = r"C:\Users\hello\OneDrive\Desktop\Wrapped Stablecoins\Gas_Prices_Data\polygon_gas_rawdata.csv"

# Load the file
df = pd.read_csv(file_path)

print(f"Data loaded successfully! Shape: {df.shape}")
print(f"Columns: {df.columns.tolist()}")

# 1. Convert DATE to datetime and remove timezone info
df['DATE'] = pd.to_datetime(df['DATE'])

# Remove timezone information if it exists
if df['DATE'].dt.tz is not None:
    df['DATE'] = df['DATE'].dt.tz_localize(None)


# 4. Set DATE as index
df = df.set_index('DATE')

# 5. Save cleaned data
output_path = r"C:\Users\hello\OneDrive\Desktop\Wrapped Stablecoins\Gas_Prices_Data\polygon_gas_cleaned.csv"
df.to_csv(output_path)
print(f"\nCleaned data saved to: {output_path}")