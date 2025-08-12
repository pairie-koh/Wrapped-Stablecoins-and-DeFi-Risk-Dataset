"""
Fetch wrapped stablecoin mint events from Covalent API
Robust version with better error handling and alternative approaches
"""

import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from dateutil import parser as dtp
from typing import Dict, List, Optional, Tuple
import json

# ------------------ CONFIG ------------------
API_KEY = os.getenv("COVALENT_API_KEY", "cqt_rQ3HBRpwkwGrwdbTXjr3DGDdkyb4")
BASE_URL = "https://api.covalenthq.com/v1"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

# Transfer event signature (keccak256 of "Transfer(address,address,uint256)")
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# Wrapped tokens to track - using lowercase addresses for consistency
TOKENS = [
    # Aave v2 (Ethereum)
    {
        "chain_id": 1,
        "chain_name": "eth-mainnet",
        "protocol": "AaveV2",
        "wrapped_symbol": "aUSDC",
        "wrapped_token_address": "0xbcca60bb61934080951369a648fb03df4f96263c",  # lowercase
        "wrapped_decimals": 6,
        "underlying_symbol": "USDC",
    },
    {
        "chain_id": 1,
        "chain_name": "eth-mainnet",
        "protocol": "AaveV2",
        "wrapped_symbol": "aUSDT",
        "wrapped_token_address": "0x3ed3b47dd13ec9a98b44e6204a523e766b225811",
        "wrapped_decimals": 6,
        "underlying_symbol": "USDT",
    },
    {
        "chain_id": 1,
        "chain_name": "eth-mainnet",
        "protocol": "AaveV2",
        "wrapped_symbol": "aDAI",
        "wrapped_token_address": "0x028171bca77440897b824ca71d1c56cac55b68a3",
        "wrapped_decimals": 18,
        "underlying_symbol": "DAI",
    },
    # Compound v2 (Ethereum)
    {
        "chain_id": 1,
        "chain_name": "eth-mainnet",
        "protocol": "CompoundV2",
        "wrapped_symbol": "cUSDC",
        "wrapped_token_address": "0x39aa39c021dfbae8fac545936693ac917d5e7563",
        "wrapped_decimals": 8,
        "underlying_symbol": "USDC",
    },
    {
        "chain_id": 1,
        "chain_name": "eth-mainnet",
        "protocol": "CompoundV2",
        "wrapped_symbol": "cDAI",
        "wrapped_token_address": "0x5d3a536e4d6dbd6114cc1ead35777bab948e3643",
        "wrapped_decimals": 8,
        "underlying_symbol": "DAI",
    },
    {
        "chain_id": 1,
        "chain_name": "eth-mainnet",
        "protocol": "CompoundV2",
        "wrapped_symbol": "cUSDT",
        "wrapped_token_address": "0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9",
        "wrapped_decimals": 8,
        "underlying_symbol": "USDT",
    },
    # Yearn v2 (Ethereum)
    {
        "chain_id": 1,
        "chain_name": "eth-mainnet",
        "protocol": "YearnV2",
        "wrapped_symbol": "yvUSDC",
        "wrapped_token_address": "0xa354f35829ae975e850e23e9615b11da1b3dc4de",  # v2 vault
        "wrapped_decimals": 6,
        "underlying_symbol": "USDC",
    },
    {
        "chain_id": 1,
        "chain_name": "eth-mainnet",
        "protocol": "YearnV2",
        "wrapped_symbol": "yvUSDT",
        "wrapped_token_address": "0x7da96a3891add058ada2e826306d812c638d87a7",
        "wrapped_decimals": 6,
        "underlying_symbol": "USDT",
    },
    {
        "chain_id": 1,
        "chain_name": "eth-mainnet",
        "protocol": "YearnV2",
        "wrapped_symbol": "yvDAI",
        "wrapped_token_address": "0xda816459f1ab5631232fe5e97a05bbbb94970c95",
        "wrapped_decimals": 18,
        "underlying_symbol": "DAI",
    },
]

# Time window
END_DT = datetime.now(timezone.utc)
START_DT = END_DT - timedelta(days=1)

# Block settings
BLOCKS_PER_DAY = 7200  # ~12s per block on Ethereum
BUFFER_BLOCKS = 500  # Smaller buffer to reduce load

# ------------------ HELPERS ------------------

def make_request(url: str, params: dict, timeout: int = 30, max_retries: int = 3) -> Optional[dict]:
    """Make API request with retry logic."""
    auth = (API_KEY, "")
    
    for attempt in range(max_retries):
        try:
            response = requests.get(
                url, 
                params=params, 
                auth=auth, 
                timeout=timeout
            )
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 504 or response.status_code == 502:
                wait_time = min(2 ** attempt, 10)
                print(f"      Gateway timeout, retry in {wait_time}s...")
                time.sleep(wait_time)
            elif response.status_code == 400:
                error_msg = response.text[:200]
                print(f"      Bad request: {error_msg}")
                return None
            else:
                print(f"      HTTP {response.status_code}")
                return None
                
        except requests.exceptions.Timeout:
            print(f"      Timeout (attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
        except Exception as e:
            print(f"      Error: {str(e)[:100]}")
            return None
    
    return None

def get_latest_block() -> int:
    """Get latest Ethereum block number."""
    url = f"{BASE_URL}/eth-mainnet/block_v2/latest/"
    
    print("  Fetching latest block...")
    data = make_request(url, {})
    
    if data and "data" in data:
        items = data["data"].get("items", [])
        if items and "height" in items[0]:
            block = int(items[0]["height"])
            print(f"  Latest block: {block:,}")
            return block
    
    # Fallback to estimate if API fails
    print("  Using estimated block number")
    return 20900000  # Update this periodically

def fetch_token_events(
    token_address: str,
    start_block: int,
    end_block: int,
    max_events: int = 1000
) -> List[dict]:
    """Fetch Transfer events for a token."""
    
    events = []
    url = f"{BASE_URL}/eth-mainnet/events/address/{token_address}/"
    
    # Process in smaller chunks to avoid timeouts
    chunk_size = 500  # Smaller chunks for problematic tokens
    current_start = start_block
    
    while current_start < end_block and len(events) < max_events:
        current_end = min(current_start + chunk_size, end_block)
        
        params = {
            "starting-block": current_start,
            "ending-block": current_end,
            "page-size": 100
        }
        
        print(f"    Blocks {current_start:,} to {current_end:,}...")
        
        # Shorter timeout for faster failure detection
        response = make_request(url, params, timeout=20)
        
        if response and "data" in response:
            items = response["data"].get("items", [])
            
            # Process each event
            for item in items:
                # Check if it's a Transfer event
                decoded = item.get("decoded", {})
                
                if decoded and decoded.get("name") == "Transfer":
                    params_list = decoded.get("params", [])
                    
                    # Extract from, to, and value
                    from_addr = None
                    to_addr = None
                    value = None
                    
                    for p in params_list:
                        name = p.get("name", "").lower()
                        val = p.get("value", "")
                        
                        if name == "from":
                            from_addr = val.lower() if val else None
                        elif name == "to":
                            to_addr = val.lower() if val else None
                        elif name in ["value", "amount"]:
                            value = val
                    
                    # Check if it's a mint (from zero address)
                    if from_addr == ZERO_ADDRESS.lower() and value:
                        try:
                            value_int = int(value)
                            if value_int > 0:  # Only include non-zero mints
                                events.append({
                                    "timestamp": item.get("block_signed_at"),
                                    "block": item.get("block_height"),
                                    "tx_hash": item.get("tx_hash"),
                                    "to_address": to_addr,
                                    "value": value_int
                                })
                        except:
                            pass
            
            print(f"      Found {len(items)} events, {len(events)} mints so far")
        
        current_start = current_end + 1
        time.sleep(0.2)  # Rate limit
    
    return events

def process_token(token: dict) -> dict:
    """Process a single token and return statistics."""
    
    symbol = token["wrapped_symbol"]
    print(f"\nProcessing {symbol} ({token['protocol']})...")
    print(f"  Address: {token['wrapped_token_address']}")
    
    try:
        # Get block range
        latest_block = get_latest_block()
        start_block = latest_block - BLOCKS_PER_DAY - BUFFER_BLOCKS
        end_block = latest_block
        
        # Fetch events
        events = fetch_token_events(
            token["wrapped_token_address"],
            start_block,
            end_block
        )
        
        if not events:
            print(f"  No mint events found")
            return {
                "protocol": token["protocol"],
                "symbol": symbol,
                "success": False,
                "error": "No mints found",
                "mints": 0,
                "volume_usd": 0
            }
        
        # Filter by timestamp
        filtered_events = []
        for event in events:
            try:
                event_time = dtp.parse(event["timestamp"]).replace(tzinfo=timezone.utc)
                if START_DT <= event_time < END_DT:
                    filtered_events.append(event)
            except:
                continue
        
        print(f"  Filtered to {len(filtered_events)} mints in last 24h")
        
        if not filtered_events:
            return {
                "protocol": token["protocol"],
                "symbol": symbol,
                "success": True,
                "error": None,
                "mints": 0,
                "volume_usd": 0
            }
        
        # Calculate statistics
        total_minted = sum(e["value"] for e in filtered_events)
        total_minted_human = total_minted / (10 ** token["wrapped_decimals"])
        
        # For stablecoins, estimate 1:1 USD value
        if token["underlying_symbol"] in ["USDC", "USDT", "DAI"]:
            volume_usd = total_minted_human
        else:
            volume_usd = 0
        
        # Create DataFrame for CSV export
        rows = []
        for event in filtered_events:
            amount = event["value"] / (10 ** token["wrapped_decimals"])
            
            rows.append({
                "timestamp": event["timestamp"],
                "block": event["block"],
                "tx_hash": event["tx_hash"],
                "protocol": token["protocol"],
                "wrapped_symbol": symbol,
                "wrapped_address": token["wrapped_token_address"],
                "underlying_symbol": token["underlying_symbol"],
                "to_address": event["to_address"],
                "amount": amount,
                "amount_usd": amount if token["underlying_symbol"] in ["USDC", "USDT", "DAI"] else 0
            })
        
        df = pd.DataFrame(rows)
        
        # Save individual token data
        filename = f"{symbol}_mints_24h.csv"
        df.to_csv(filename, index=False)
        print(f"  Saved {len(df)} mints to {filename}")
        
        return {
            "protocol": token["protocol"],
            "symbol": symbol,
            "success": True,
            "error": None,
            "mints": len(filtered_events),
            "volume_usd": volume_usd,
            "unique_addresses": df["to_address"].nunique() if not df.empty else 0,
            "dataframe": df
        }
        
    except Exception as e:
        print(f"  ERROR: {str(e)[:200]}")
        return {
            "protocol": token["protocol"],
            "symbol": symbol,
            "success": False,
            "error": str(e)[:100],
            "mints": 0,
            "volume_usd": 0
        }

# ------------------ MAIN ------------------

def main():
    """Main execution."""
    
    print("=" * 70)
    print("WRAPPED STABLECOIN MINTS TRACKER")
    print("=" * 70)
    print(f"Time range: {START_DT.strftime('%Y-%m-%d %H:%M')} to {END_DT.strftime('%Y-%m-%d %H:%M')} UTC")
    print(f"Tracking {len(TOKENS)} tokens")
    print("=" * 70)
    
    results = []
    all_dataframes = []
    
    # Process each token
    for token in TOKENS:
        result = process_token(token)
        results.append(result)
        
        if result["success"] and "dataframe" in result:
            all_dataframes.append(result["dataframe"])
    
    # Combine all data
    if all_dataframes:
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        combined_df.sort_values(["timestamp", "protocol", "wrapped_symbol"], inplace=True)
        combined_df.to_csv("all_wrapped_mints_24h.csv", index=False)
        print(f"\nCombined data saved to all_wrapped_mints_24h.csv")
    
    # Print summary
    print("\n" + "=" * 70)
    print("SUMMARY REPORT")
    print("=" * 70)
    
    # Success/failure counts
    successful = [r for r in results if r["success"]]
    failed = [r for r in results if not r["success"]]
    
    print(f"Successful: {len(successful)}/{len(results)} tokens")
    print(f"Failed: {len(failed)}/{len(results)} tokens")
    
    if failed:
        print("\nFailed tokens:")
        for r in failed:
            print(f"  - {r['symbol']} ({r['protocol']}): {r['error']}")
    
    if successful:
        print("\nSuccessful tokens:")
        print(f"{'Protocol':<15} {'Token':<10} {'Mints':>8} {'Volume USD':>15} {'Unique Addrs':>12}")
        print("-" * 70)
        
        total_mints = 0
        total_volume = 0
        
        for r in successful:
            if r["mints"] > 0:
                print(f"{r['protocol']:<15} {r['symbol']:<10} {r['mints']:>8} ${r['volume_usd']:>14,.2f} {r.get('unique_addresses', 0):>12}")
                total_mints += r["mints"]
                total_volume += r["volume_usd"]
        
        print("-" * 70)
        print(f"{'TOTAL':<26} {total_mints:>8} ${total_volume:>14,.2f}")
    
    print("=" * 70)
    print("\nNote: USD values are estimated 1:1 for stablecoins")
    print("Individual token CSVs have been created for detailed analysis")

if __name__ == "__main__":
    main()