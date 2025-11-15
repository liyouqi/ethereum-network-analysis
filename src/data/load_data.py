import pandas as pd
import os

PROCESSED_PATH = "data/processed/"

def load_clean_transactions():
    return pd.read_csv(PROCESSED_PATH + "transactions_clean.csv")

def load_clean_token_transfers():
    return pd.read_csv(PROCESSED_PATH + "token_transfers_clean.csv")
