-- Move old parts to the S3Proxy-backed `cold` volume on tables created before
-- tiering was added. New installs get this from 01/03/05; this file makes
-- `make schema` re-apply it. Existing parts stay on the hot volume and move
-- when their TTL rule runs. MODIFY SETTING and MODIFY TTL are separate ALTERs
-- because ClickHouse does not parse them as one action list.
ALTER TABLE analytics.equities_prices_local ON CLUSTER analytics_cluster
    MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE analytics.equities_prices_local ON CLUSTER analytics_cluster
    MODIFY TTL toDateTime(ts) + INTERVAL 30 DAY TO VOLUME 'cold',
        toDateTime(ts) + INTERVAL 90 DAY DELETE;
ALTER TABLE analytics.crypto_trades_local ON CLUSTER analytics_cluster
    MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE analytics.crypto_trades_local ON CLUSTER analytics_cluster
    MODIFY TTL toDateTime(timestamp) + INTERVAL 7 DAY TO VOLUME 'cold',
        toDateTime(timestamp) + INTERVAL 90 DAY DELETE;
ALTER TABLE analytics.crypto_ohlcv_local ON CLUSTER analytics_cluster
    MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE analytics.crypto_ohlcv_local ON CLUSTER analytics_cluster
    MODIFY TTL toDateTime(timestamp) + INTERVAL 7 DAY TO VOLUME 'cold',
        toDateTime(timestamp) + INTERVAL 90 DAY DELETE;
ALTER TABLE analytics.crypto_market_data_local ON CLUSTER analytics_cluster
    MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE analytics.crypto_market_data_local ON CLUSTER analytics_cluster
    MODIFY TTL toDateTime(timestamp) + INTERVAL 7 DAY TO VOLUME 'cold',
        toDateTime(timestamp) + INTERVAL 90 DAY DELETE;
ALTER TABLE analytics.forex_rates_local ON CLUSTER analytics_cluster
    MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE analytics.forex_rates_local ON CLUSTER analytics_cluster
    MODIFY TTL toDateTime(timestamp) + INTERVAL 7 DAY TO VOLUME 'cold',
        toDateTime(timestamp) + INTERVAL 90 DAY DELETE;
ALTER TABLE analytics.macro_indicators_local ON CLUSTER analytics_cluster
    MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE analytics.macro_indicators_local ON CLUSTER analytics_cluster
    MODIFY TTL toDate(timestamp) + INTERVAL 1 YEAR TO VOLUME 'cold',
        toDate(timestamp) + INTERVAL 10 YEAR DELETE;
ALTER TABLE analytics.company_filings_local ON CLUSTER analytics_cluster
    MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE analytics.company_filings_local ON CLUSTER analytics_cluster
    MODIFY TTL toDate(filing_date) + INTERVAL 1 YEAR TO VOLUME 'cold',
        toDate(filing_date) + INTERVAL 10 YEAR DELETE;
ALTER TABLE analytics.corporate_actions_local ON CLUSTER analytics_cluster
    MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE analytics.corporate_actions_local ON CLUSTER analytics_cluster
    MODIFY TTL toDate(action_date) + INTERVAL 1 YEAR TO VOLUME 'cold',
        toDate(action_date) + INTERVAL 10 YEAR DELETE;
ALTER TABLE analytics.fundamentals_local ON CLUSTER analytics_cluster
    MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE analytics.fundamentals_local ON CLUSTER analytics_cluster
    MODIFY TTL toDate(end_date) + INTERVAL 1 YEAR TO VOLUME 'cold',
        toDate(end_date) + INTERVAL 10 YEAR DELETE;
ALTER TABLE analytics.insider_transactions_local ON CLUSTER analytics_cluster
    MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE analytics.insider_transactions_local ON CLUSTER analytics_cluster
    MODIFY TTL toDate(transaction_date) + INTERVAL 1 YEAR TO VOLUME 'cold',
        toDate(transaction_date) + INTERVAL 10 YEAR DELETE;
ALTER TABLE analytics.pipeline_errors_local ON CLUSTER analytics_cluster
    MODIFY SETTING storage_policy = 'tiered';
ALTER TABLE analytics.pipeline_errors_local ON CLUSTER analytics_cluster
    MODIFY TTL toDateTime(ts) + INTERVAL 7 DAY TO VOLUME 'cold',
        toDateTime(ts) + INTERVAL 30 DAY DELETE;

-- Reference snapshots are republished daily; older versions are useless.
ALTER TABLE analytics.symbol_catalog_local ON CLUSTER analytics_cluster
    MODIFY TTL toDateTime(ingested_at) + INTERVAL 30 DAY;
