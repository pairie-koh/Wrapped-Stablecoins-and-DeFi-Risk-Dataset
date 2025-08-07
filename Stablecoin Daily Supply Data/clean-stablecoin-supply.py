import pandas as pd
from datetime import datetime

def create_clean_long_format_csv(input_filename='area-chart-data-2025-08-04.csv', 
                                output_filename='stablecoins_2020_2025_long.csv'):
    """
    Create a single cleaned CSV in long format with data from 2020-01-01 to 2025-08-02
    Only includes: Date, Stablecoin, and Circulation columns
    Only includes: USDT, USDC, DAI, BUSD, TUSD
    """
    
    print("Creating cleaned long-format stablecoin data")
    print("=" * 60)
    
    # Read the CSV
    df = pd.read_csv(input_filename)
    
    # Convert Date column to datetime
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Define the stablecoins we want
    target_stablecoins = ['USDT', 'USDC', 'DAI', 'BUSD', 'TUSD']
    
    # Filter for date range: 2020-01-01 to 2025-08-02 (inclusive)
    start_date = '2020-01-01'
    end_date = '2025-08-02'
    
    mask = (df['Date'] >= start_date) & (df['Date'] <= end_date)
    filtered_df = df.loc[mask].copy()
    
    print(f"\nFiltering data from {start_date} to {end_date} (inclusive)")
    print(f"Total days in range: {len(filtered_df)}")
    
    # Keep only Date and the target stablecoin columns
    columns_to_keep = ['Date'] + [col for col in target_stablecoins if col in df.columns]
    filtered_df = filtered_df[columns_to_keep]
    
    # Replace any missing values with 0
    for col in target_stablecoins:
        if col in filtered_df.columns:
            filtered_df[col] = pd.to_numeric(filtered_df[col], errors='coerce').fillna(0)
    
    # Convert to long format
    long_df = pd.melt(filtered_df, 
                      id_vars=['Date'], 
                      value_vars=[col for col in target_stablecoins if col in filtered_df.columns],
                      var_name='Stablecoin', 
                      value_name='Circulation')
    
    # Sort by date and stablecoin for better organization
    long_df = long_df.sort_values(['Date', 'Stablecoin']).reset_index(drop=True)
    
    # Save to CSV
    long_df.to_csv(output_filename, index=False)
    
    print(f"\nSaved to: {output_filename}")
    print(f"Total rows: {len(long_df):,}")
    print(f"Columns: {list(long_df.columns)}")
    
    # Show sample of the data
    print("\nFirst 10 rows:")
    print(long_df.head(10))
    
    print("\nLast 10 rows:")
    print(long_df.tail(10))
    
    # Summary statistics
    print("\nSummary:")
    print("-" * 60)
    unique_dates = long_df['Date'].nunique()
    unique_stablecoins = long_df['Stablecoin'].unique()
    
    print(f"Date range: {long_df['Date'].min()} to {long_df['Date'].max()}")
    print(f"Number of dates: {unique_dates:,}")
    print(f"Stablecoins included: {', '.join(unique_stablecoins)}")
    print(f"Rows per stablecoin: {len(long_df) // len(unique_stablecoins):,}")
    
    return long_df

if __name__ == "__main__":
    # Create the single cleaned long-format CSV
    # Using raw string (r"...") to handle Windows file paths properly
    input_file = r"C:\Users\hello\OneDrive\Desktop\Wrapped Stablecoins\Stablecoin Supply Data\Raw-Data All Stablecoin Supplies.csv"
    output_file = r"C:\Users\hello\OneDrive\Desktop\Wrapped Stablecoins\Stablecoin Supply Data\stablecoins_2020_2025_long.csv"
    
    create_clean_long_format_csv(
        input_filename=input_file,
        output_filename=output_file
    )

    