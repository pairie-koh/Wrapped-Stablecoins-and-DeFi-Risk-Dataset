import pandas as pd

# Step 1: Load the file
df = pd.read_csv(r"C:\Users\hello\Downloads\BTC_USD Bitfinex Historical Data.csv")

# Step 2: Strip quotes, commas, and whitespace
df.columns = df.columns.str.strip()
df = df.applymap(lambda x: str(x).replace('"', '').replace(',', '').strip() if isinstance(x, str) else x)

# Step 3: Convert columns to numeric (no renaming)
numeric_cols = ['"Open"', '"High"', '"Low"', '"Price"']
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

# Step 4: Convert "Vol." if present
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

# Step 5: Convert "Date" column to datetime and sort
if '"Date"' in df.columns:
    df['"Date"'] = pd.to_datetime(df['"Date"'], errors='coerce')
    df.sort_values('"Date"', inplace=True)

# Step 6: Export cleaned CSV
df.to_csv("cleaned_bitfinex_rawnames.csv", index=False)
print("✅ Cleaned file saved as 'cleaned_bitfinex_rawnames.csv'")
