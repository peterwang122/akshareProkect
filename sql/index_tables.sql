CREATE TABLE IF NOT EXISTS index_basic_info (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  index_code VARCHAR(16) NOT NULL COMMENT 'Raw index code from AKShare, e.g. sh000001',
  simple_code VARCHAR(16) NULL COMMENT 'Numeric index code, e.g. 000001',
  market VARCHAR(8) NULL COMMENT 'Market prefix, e.g. sh or sz',
  index_name VARCHAR(64) NOT NULL COMMENT 'Index name',
  data_source VARCHAR(32) NOT NULL DEFAULT 'akshare' COMMENT 'Data source',
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_index_code (index_code),
  KEY idx_simple_code_market (simple_code, market),
  KEY idx_index_name (index_name)
) COMMENT='Index basic information from AKShare';

CREATE TABLE IF NOT EXISTS index_daily_data (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  index_code VARCHAR(16) NOT NULL COMMENT 'Raw index code from AKShare, e.g. sh000001',
  open_price DECIMAL(12, 4) NULL COMMENT 'Open price',
  close_price DECIMAL(12, 4) NULL COMMENT 'Close price',
  high_price DECIMAL(12, 4) NULL COMMENT 'High price',
  low_price DECIMAL(12, 4) NULL COMMENT 'Low price',
  volume DECIMAL(20, 2) NULL COMMENT 'Volume',
  turnover DECIMAL(22, 2) NULL COMMENT 'Turnover amount',
  amplitude DECIMAL(8, 4) NULL COMMENT 'Amplitude, percent',
  price_change_rate DECIMAL(8, 4) NULL COMMENT 'Price change rate, percent',
  price_change_amount DECIMAL(12, 4) NULL COMMENT 'Price change amount',
  turnover_rate DECIMAL(8, 4) NULL COMMENT 'Turnover rate, percent',
  trade_date DATE NOT NULL COMMENT 'Trading date',
  data_source VARCHAR(32) NOT NULL DEFAULT 'akshare' COMMENT 'Data source',
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_index_code_trade_date (index_code, trade_date),
  KEY idx_trade_date (trade_date),
  KEY idx_index_code (index_code)
) COMMENT='Daily index market data from AKShare';
