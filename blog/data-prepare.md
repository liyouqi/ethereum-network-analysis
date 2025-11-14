---
title: "Ehtereum Network Analysis - 1. Data Preparation"
date: 2025-11-13
categories:
  - Data Mining
tags:
  - Reflection
  - Cryptocurrency
  - Ethereum
  - Blockchain
  - Data pareparation
  - Data Science
layout: single
author_profile: true
read_time: true
comments: false
share: true
---

## Data Source Selection
There are many ways to explore Ethereum data on internet, genereally through blockchain explorers or public datasets. Kaggle is a good choice for datasets which is prepared and cleaned, but I can not find the newest Ethereum data there. What's more, I need different data such as token transfers, contracts, blocks, etc and all of these data should be filtered by specific time period and value threshold, so I decide to use some retrieval tools to get consistent data by myself. After searching and asking ChatGPT, I find Google BigQuery eventually meets my needs.

# Data Source: 
Google BigQuery Public Datasets - [Crypto Ethereum](https://console.cloud.google.com/marketplace/product/bigquery-public-data/crypto_ethereum)


# Data selection criteria:
if I have a high-performance computer with enough storage space, I can esily just Seletct * from the whole dataset. But with a laptop, I need to trade-off in order to get manageable data size. 

That is tough. Even for one week data, the size of raw data is more than 1TB. Capacity is managable, but the processsing phase may take too long time. So I decide to limit the time period to one day, and only select high-value transactions (whale transactions) for analysis. Yes, you know, how huch shitcoins and rubbish tokens are there in Ethereum network. 
To avoid too much noise, I only focus on stablecoins (USDC, USDT) and wrapped ETH (WETH) for token transfers, and ETH value for normal transactions.

#### in brief:
- Time period: 2025-11-11 00:00:00 to 2025-11-12 00:00:00 (24 hours)
- Value threshold: $10,000 USD equivalent   
- Tokens: USDC, USDT, WETH (for stablecoins and wrapped ETH)
- Data tables: token_transfers, transactions, blocks, contracts
- Data cleaning: remove nested/complex fields for easier export
- Export format: CSV (for compatibility with various analysis tools)
- Data storage: Google BigQuery (for large-scale data handling)


##### Some notes on BigQuery:
- Be careful with reserved keywords **hash**, if needed, use backticks
- If the file size is too large e.g. >1GB, you can not download it directly, you need to export it to Google Cloud Storage first, then download from there. Another trick is to open a querry editor in BigQuery console, run a SELECT * query on the table, and then use the "Save Results" option to download a sample of the data directly. Then, you don't have to bother with credit card and billing settings.

I'll share the SQL code bolow and upload the row data files to GitHub repo.

## Data Preparation Code
```sql
-- =============================
-- Fixed-time period data extraction
-- =============================
DECLARE T0 DEFAULT TIMESTAMP("2025-11-11 00:00:00");
DECLARE T1 DEFAULT TIMESTAMP("2025-11-12 00:00:00");

-------------------------------------------------------------------------------
-- 1) token_transfers_filtered (USDC / USDT / WETH)
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `ethereumanalysis-478200.ethereum_us.token_transfers_filtered` AS
WITH stable_tokens AS (
  SELECT *
  FROM `bigquery-public-data.crypto_ethereum.token_transfers`
  WHERE 
    block_timestamp BETWEEN T0 AND T1
    AND token_address IN (
      '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', -- USDC
      '0xdac17f958d2ee523a2206206994597c13d831ec7', -- USDT
      '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'  -- WETH
    )
)

SELECT
  transaction_hash,
  from_address,
  to_address,
  block_number,
  token_address,
  CAST(value AS NUMERIC) AS raw_value,
  block_timestamp,

  -- USD conversion
  CASE
    WHEN token_address IN (
      '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
      '0xdac17f958d2ee523a2206206994597c13d831ec7'
    )
      THEN CAST(value AS NUMERIC) / 1e6 -- USDC/USDT decimals
    WHEN token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
      THEN (CAST(value AS NUMERIC) / 1e18) * 3000 -- WETH
    ELSE NULL
  END AS usd_value

FROM stable_tokens
WHERE 
      (token_address IN (
        '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
        '0xdac17f958d2ee523a2206206994597c13d831ec7'
      ) AND (CAST(value AS NUMERIC) / 1e6) > 10000)
   OR (
        token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        AND ((CAST(value AS NUMERIC) / 1e18) * 3000) > 10000
      );

-------------------------------------------------------------------------------
-- 2) transactions_filtered (ETH > $10k)
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `ethereumanalysis-478200.ethereum_us.transactions_filtered` AS
WITH eth_tx AS (
  SELECT
    `hash`,        
    from_address,
    to_address,
    block_number,
    value,
    block_timestamp
  FROM `bigquery-public-data.crypto_ethereum.transactions`
  WHERE block_timestamp BETWEEN T0 AND T1
)

SELECT *
FROM eth_tx
WHERE (value / 1e18) * 3000 > 10000;  -- ETH whale ($10k)

-------------------------------------------------------------------------------
-- 3) blocks_filtered_export 
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `ethereumanalysis-478200.ethereum_us.blocks_filtered_export` AS
SELECT
    number,
    `hash`,
    parent_hash,
    nonce,
    sha3_uncles,
    logs_bloom,
    transactions_root,
    state_root,
    receipts_root,
    miner,
    difficulty,
    total_difficulty,
    size,
    extra_data,
    gas_limit,
    gas_used,
    timestamp,
    transaction_count
FROM `bigquery-public-data.crypto_ethereum.blocks`
WHERE timestamp BETWEEN T0 AND T1
  AND number IN (
    SELECT block_number FROM `ethereumanalysis-478200.ethereum_us.transactions_filtered`
    UNION DISTINCT
    SELECT block_number FROM `ethereumanalysis-478200.ethereum_us.token_transfers_filtered`
  );

-------------------------------------------------------------------------------
-- 4) contracts_filtered_export (all struct/array fields converted to JSON strings)
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `ethereumanalysis-478200.ethereum_us.contracts_filtered_export` AS
WITH addr AS (
  SELECT from_address AS a FROM `ethereumanalysis-478200.ethereum_us.transactions_filtered`
  UNION DISTINCT
  SELECT to_address AS a FROM `ethereumanalysis-478200.ethereum_us.transactions_filtered`
  UNION DISTINCT
  SELECT from_address AS a FROM `ethereumanalysis-478200.ethereum_us.token_transfers_filtered`
  UNION DISTINCT
  SELECT to_address AS a FROM `ethereumanalysis-478200.ethereum_us.token_transfers_filtered`
)

SELECT
  address,
  is_erc20,
  is_erc721,
  block_number,
  -- for struct/array fields, convert to JSON strings
  TO_JSON_STRING(bytecode)            AS bytecode_json,
  TO_JSON_STRING(function_sighashes)  AS function_sighashes_json
FROM `bigquery-public-data.crypto_ethereum.contracts`
WHERE address IN (SELECT a FROM addr);

```
