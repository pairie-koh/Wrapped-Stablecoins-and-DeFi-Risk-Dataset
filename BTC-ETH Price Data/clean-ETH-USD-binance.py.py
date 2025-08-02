import pandas as pd

df = pd.read_csv(r"C:\Users\hello\Downloads\ETH_USD Binance Historical Data.csv")

# Strip quotes, commas, and whitespace
df.columns = df.columns.str.strip()
df = df.applymap(lambda x: str(x).replace('"', '').replace(',', '').strip() if isinstance(x, str) else x)

# Convert columns to numeric 
numeric_cols = ['"Open"', '"High"', '"Low"', '"Price"']
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

# convert vol.
def parse_volume(val):
    if isinstance(val, str):
        val = val.upper().replace('K', 'e3').replace('M', 'e6')
        try:
            return float(eval(val))
        except:
            return None
    return val

if '"Vol."' in df.columns:
    df['"Vol."'] = df['"Vol."'].apply(parse_volume)

# converted date to datetime 
if '"Date"' in df.columns:
    df['"Date"'] = pd.to_datetime(df['"Date"'], errors='coerce')
    df.sort_values('"Date"', inplace=True)

# export cleaned csv 
df.to_csv("ETH-USD-binance.csv", index=False)
print("Cleaned file has been saved")
