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
