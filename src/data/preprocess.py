import pandas as pd
import numpy as np

# global invalid addresses
INVALID_ADDRESSES = {
    "0x0000000000000000000000000000000000000000",
    "0x000000000000000000000000000000000000dead"
}

def normalize_addresses(df, cols):
    """uniformly format Ethereum addresses to lowercase and strip whitespace"""
    for c in cols:
        df[c] = df[c].astype(str).str.lower().str.strip()
        df = df[~df[c].isin(INVALID_ADDRESSES)]
    return df


def clean_transactions(df):
    """clean transaction data"""
    print("Initial shape:", df.shape)
    # 1. address normalization
    df = df.dropna(subset=["from_address", "to_address"])
    df = normalize_addresses(df, ["from_address", "to_address"])

    # 2. timpestamp conversion
    if "block_timestamp" in df.columns:
        df["block_timestamp"] = pd.to_datetime(df["block_timestamp"], errors="coerce")
    
    # 3. remove zero value transactions, reduce noise
    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["value"])
        df = df[df["value"] > 0]

    # 4. remove failed transactions
    if "status" in df.columns:
        df = df[df["status"] == 1]

    return df


def clean_token_transfers(df):
    """clean token transfer data"""

    df = df.dropna(subset=["from_address", "to_address", "token_address"])
    df = normalize_addresses(df, ["from_address", "to_address", "token_address"])

    if "block_timestamp" in df.columns:
        df["block_timestamp"] = pd.to_datetime(df["block_timestamp"], errors="coerce")

    # Ensure numeric 'value' before comparisons to avoid TypeError
    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["value"])
        df = df[df["value"] > 0]

    return df
