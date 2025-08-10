

# Quick one-liner to get unique bridge protocols
import pandas as pd

# Load your CSV
df = pd.read_csv(r"C:\Users\hello\OneDrive\Desktop\Wrapped Stablecoins\2021 bridge data.csv")

# Get unique protocols
unique_protocols = df['BRIDGE_PROTOCOL'].unique()
print("Unique Bridge Protocols:")
print("-" * 30)
for protocol in sorted(unique_protocols):
    print(f"- {protocol}")

# Or get counts
protocol_counts = df['BRIDGE_PROTOCOL'].value_counts()
print(f"\nProtocol Counts:")
print(protocol_counts)