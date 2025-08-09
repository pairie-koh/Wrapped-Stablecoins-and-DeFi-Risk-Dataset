# pip install requests pandas python-dateutil rapidfuzz
import requests
import pandas as pd
from datetime import timezone
from rapidfuzz import process, fuzz

START = "2020-01-01"
END   = "2025-08-02"

# Put what YOU think the names/slugs are; we'll resolve them:
WANTED = ["aave", "compound", "makerdao"]

BASE = "https://api.llama.fi"

def get_protocol_catalog():
    r = requests.get(f"{BASE}/protocols", timeout=60)
    r.raise_for_status()
    cat = r.json()
    # map slug -> full object, and also name->slug for matching
    by_slug = {p["slug"]: p for p in cat if "slug" in p}
    names = {p.get("name",""): p["slug"] for p in cat if "slug" in p}
    return by_slug, names

def resolve_slug(wanted, by_slug, names, score_cut=75):
    # Exact slug
    if wanted in by_slug:
        return wanted
    # Exact name
    if wanted in names:
        return names[wanted]
    # Fuzzy over slugs + names
    choices = list(by_slug.keys()) + list(names.keys())
    match, score, _ = process.extractOne(wanted, choices, scorer=fuzz.token_sort_ratio)
    if score >= score_cut:
        # if match is a name, map to slug
        return names.get(match, match)
    return None

def fetch_protocol(slug):
    url = f"{BASE}/protocol/{slug}"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json()

def normalize_points(points):
    """Return DataFrame with datetime,tvl_usd from a list/dict points."""
    if isinstance(points, list):
        df = pd.DataFrame(points)
    elif isinstance(points, dict) and "tvl" in points:
        df = pd.DataFrame(points["tvl"])
    else:
        return pd.DataFrame(columns=["datetime","tvl_usd"])
    # normalize keys
    if "date" not in df.columns:
        return pd.DataFrame(columns=["datetime","tvl_usd"])
    if "totalLiquidityUSD" in df.columns:
        tvl_col = "totalLiquidityUSD"
    elif "tvl" in df.columns:
        tvl_col = "tvl"
    elif "tvlUsd" in df.columns:
        tvl_col = "tvlUsd"
    else:
        # fall back to a likely numeric column
        num_cols = [c for c in df.columns if c != "date" and pd.api.types.is_numeric_dtype(df[c])]
        tvl_col = num_cols[0] if num_cols else None
    if tvl_col is None:
        return pd.DataFrame(columns=["datetime","tvl_usd"])
    df["datetime"] = pd.to_datetime(df["date"], unit="s", utc=True)
    df = df[["datetime", tvl_col]].rename(columns={tvl_col: "tvl_usd"})
    return df

def hourlyize(df, start=START, end=END, fill="ffill"):
    if df.empty:
        return df
    full_idx = pd.date_range(
        start=pd.Timestamp(start, tz=timezone.utc),
        end=pd.Timestamp(end, tz=timezone.utc) + pd.Timedelta(days=1) - pd.Timedelta(hours=1),
        freq="h",  # <- lowercase 'h'
    )
    out = df.set_index("datetime").sort_index().reindex(full_idx)
    if fill == "ffill":
        out["tvl_usd"] = out["tvl_usd"].ffill()
    elif fill == "interpolate":
        out["tvl_usd"] = out["tvl_usd"].interpolate(method="time", limit_direction="both")
    out.index.name = "datetime"
    return out.reset_index()

def main():
    by_slug, names = get_protocol_catalog()

    resolved = []
    for want in WANTED:
        slug = resolve_slug(want, by_slug, names)
        if slug:
            print(f"[OK] {want}  slug: {slug}")
            resolved.append(slug)
        else:
            print(f"[WARN] Could not resolve '{want}' to a DeFiLlama slug")

    total_out = []
    chain_out = []

    for slug in resolved:
        try:
            j = fetch_protocol(slug)
        except requests.HTTPError as e:
            print(f"[WARN] Fetch failed for {slug}: {e}")
            continue

        # Protocol-level series
        if isinstance(j.get("tvl"), list):
            df_total = normalize_points(j["tvl"])
            df_total["protocol"] = slug
            total_out.append(hourlyize(df_total))

        # Per-chain series can be:
        # - chainTvls: { chainName: [points] }  OR
        # - chainTvls: { chainName: { tvl: [points], tokenBreakdowns: ... } }
        ctv = j.get("chainTvls")
        if isinstance(ctv, dict):
            for chain_name, payload in ctv.items():
                df_chain = normalize_points(payload)
                if not df_chain.empty:
                    df_chain["protocol"] = slug
                    df_chain["chain"] = chain_name
                    chain_out.append(hourlyize(df_chain))

    if total_out:
        total_df = pd.concat(total_out, ignore_index=True)
        total_df = total_df[["datetime","protocol","tvl_usd"]].sort_values(["protocol","datetime"])
        total_df.to_csv("tvl_hourly_total.csv", index=False)
        print("Saved tvl_hourly_total.csv")
    else:
        print("[INFO] No protocol-level TVL rows collected.")

    if chain_out:
        chain_df = pd.concat(chain_out, ignore_index=True)
        chain_df = chain_df[["datetime","protocol","chain","tvl_usd"]].sort_values(["protocol","chain","datetime"])
        chain_df.to_csv("tvl_hourly_by_chain.csv", index=False)
        print("Saved tvl_hourly_by_chain.csv")
    else:
        print("[INFO] No per-chain TVL rows collected.")

if __name__ == "__main__":
    main()
