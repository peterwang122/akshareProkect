-- Switch existing US index futures tables from monthly contracts to Sina continuous series.
-- Run once on databases where futures_us_index_* tables already existed before this change.

ALTER TABLE futures_us_index_contract_info
  MODIFY contract_month VARCHAR(16) NULL COMMENT 'Contract month or CONTINUOUS',
  MODIFY exchange VARCHAR(32) NULL COMMENT 'Quote source, e.g. SINA',
  MODIFY data_source VARCHAR(64) NOT NULL DEFAULT 'sina_global_futures';

ALTER TABLE futures_us_index_daily_data
  MODIFY contract_month VARCHAR(16) NULL COMMENT 'Contract month or CONTINUOUS',
  MODIFY close_price DECIMAL(18, 6) NULL COMMENT 'Close price from Sina daily K line',
  MODIFY closing_range_raw VARCHAR(64) NULL COMMENT 'Unused for Sina global futures',
  MODIFY data_source VARCHAR(64) NOT NULL DEFAULT 'sina_global_futures';
