CREATE TABLE IF NOT EXISTS forex_basic_info (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  symbol_code VARCHAR(32) NOT NULL COMMENT 'Forex symbol code from forex_spot_em, e.g. USDCNH',
  symbol_name VARCHAR(128) NULL COMMENT 'Forex symbol name',
  data_source VARCHAR(32) NOT NULL DEFAULT 'forex_spot_em' COMMENT 'Data source',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_symbol_code (symbol_code),
  KEY idx_symbol_name (symbol_name)
) COMMENT='Forex symbol basic information from AKShare';

CREATE TABLE IF NOT EXISTS forex_daily_data (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  symbol_code VARCHAR(32) NOT NULL COMMENT 'Forex symbol code',
  symbol_name VARCHAR(128) NULL COMMENT 'Forex symbol name',
  trade_date DATE NOT NULL COMMENT 'Trading date',
  open_price DECIMAL(18, 6) NULL COMMENT 'Open price',
  latest_price DECIMAL(18, 6) NULL COMMENT 'Latest/close price',
  high_price DECIMAL(18, 6) NULL COMMENT 'High price',
  low_price DECIMAL(18, 6) NULL COMMENT 'Low price',
  amplitude DECIMAL(12, 6) NULL COMMENT 'Amplitude, percent',
  data_source VARCHAR(32) NOT NULL DEFAULT 'forex_hist_em' COMMENT 'Data source',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_symbol_code_trade_date (symbol_code, trade_date),
  KEY idx_trade_date (trade_date),
  KEY idx_symbol_code (symbol_code)
) COMMENT='Forex historical daily data from AKShare';
