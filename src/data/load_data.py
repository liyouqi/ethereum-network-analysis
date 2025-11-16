import pandas as pd
import os, sys


CURRENT_DIR = os.path.dirname(__file__)

PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, '..', '..'))

PROCESSED_PATH = os.path.join(PROJECT_ROOT, 'data', 'processed')
RAW_PATH = os.path.join(PROJECT_ROOT, 'data', 'raw')
# print("Processed data path:", PROCESSED_PATH)
# print("Raw data path:", RAW_PATH)
TOKEN_TX_STRING_COLS = [
    "transaction_hash",
    "from_address",
    "to_address",
    "block_number",
    "token_address",
    "raw_value",
    "block_timestamp",
    "user_value"
]

def load_clean_transactions():
    return pd.read_csv(os.path.join(PROCESSED_PATH, "transactions_clean.csv"))

def load_clean_token_transfers():
    path = os.path.join(PROCESSED_PATH, "token_transfers_clean.csv")
    return pd.read_csv(path, dtype={c: "string" for c in TOKEN_TX_STRING_COLS}, low_memory=False)

def load_raw_contracts():
    return pd.read_csv(os.path.join(RAW_PATH, "contracts_filtered_export.csv"))
