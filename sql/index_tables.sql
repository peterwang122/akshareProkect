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

CREATE TABLE IF NOT EXISTS index_us_basic_info (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  index_code VARCHAR(16) NOT NULL COMMENT 'US index code from AKShare, e.g. .IXIC',
  simple_code VARCHAR(16) NULL COMMENT 'Simple symbol, e.g. IXIC',
  market VARCHAR(8) NULL COMMENT 'Market prefix, e.g. us',
  index_name VARCHAR(64) NOT NULL COMMENT 'Index name',
  data_source VARCHAR(32) NOT NULL DEFAULT 'akshare' COMMENT 'Data source',
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_index_us_code (index_code),
  KEY idx_index_us_simple_code_market (simple_code, market),
  KEY idx_index_us_name (index_name)
) COMMENT='US index basic information from AKShare';

CREATE TABLE IF NOT EXISTS index_us_daily_data (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  index_code VARCHAR(16) NOT NULL COMMENT 'US index code from AKShare, e.g. .IXIC',
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
  UNIQUE KEY uk_index_us_code_trade_date (index_code, trade_date),
  KEY idx_index_us_trade_date (trade_date),
  KEY idx_index_us_code (index_code)
) COMMENT='Daily US index market data from AKShare';

CREATE TABLE IF NOT EXISTS index_hk_basic_info (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  index_code VARCHAR(16) NOT NULL COMMENT 'HK index code from AKShare, e.g. hkHSI',
  simple_code VARCHAR(16) NULL COMMENT 'Simple symbol, e.g. HSI',
  market VARCHAR(8) NULL COMMENT 'Market prefix, e.g. hk',
  index_name VARCHAR(64) NOT NULL COMMENT 'Index name',
  data_source VARCHAR(32) NOT NULL DEFAULT 'akshare' COMMENT 'Data source',
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_index_hk_code (index_code),
  KEY idx_index_hk_simple_code_market (simple_code, market),
  KEY idx_index_hk_name (index_name)
) COMMENT='HK index basic information from AKShare';

CREATE TABLE IF NOT EXISTS index_hk_daily_data (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  index_code VARCHAR(16) NOT NULL COMMENT 'HK index code from AKShare, e.g. hkHSI',
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
  UNIQUE KEY uk_index_hk_code_trade_date (index_code, trade_date),
  KEY idx_index_hk_trade_date (trade_date),
  KEY idx_index_hk_code (index_code)
) COMMENT='Daily HK index market data from AKShare';

CREATE TABLE IF NOT EXISTS index_qvix_basic_info (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  index_code VARCHAR(32) NOT NULL COMMENT 'QVIX index code, e.g. 50ETF_QVIX',
  simple_code VARCHAR(32) NULL COMMENT 'Simple symbol, e.g. 50ETF',
  market VARCHAR(8) NULL COMMENT 'Market prefix, e.g. cn',
  index_name VARCHAR(64) NOT NULL COMMENT 'Index name',
  data_source VARCHAR(32) NOT NULL DEFAULT 'akshare' COMMENT 'Data source',
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_index_qvix_code (index_code),
  KEY idx_index_qvix_simple_code_market (simple_code, market),
  KEY idx_index_qvix_name (index_name)
) COMMENT='QVIX index basic information from AKShare';

CREATE TABLE IF NOT EXISTS index_qvix_daily_data (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  index_code VARCHAR(32) NOT NULL COMMENT 'QVIX index code, e.g. 50ETF_QVIX',
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
  UNIQUE KEY uk_index_qvix_code_trade_date (index_code, trade_date),
  KEY idx_index_qvix_trade_date (trade_date),
  KEY idx_index_qvix_code (index_code)
) COMMENT='Daily QVIX index market data from AKShare';

CREATE TABLE IF NOT EXISTS index_news_sentiment_scope_daily (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  trade_date DATE NOT NULL COMMENT 'Trading date',
  sentiment_value DECIMAL(12, 4) NULL COMMENT 'Market sentiment value',
  hs300_close DECIMAL(12, 4) NULL COMMENT 'HS300 close value',
  data_source VARCHAR(32) NOT NULL DEFAULT 'akshare' COMMENT 'Data source',
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_index_news_sentiment_scope_trade_date (trade_date),
  KEY idx_index_news_sentiment_scope_trade_date (trade_date)
) COMMENT='Daily market news sentiment scope data from AKShare';

CREATE TABLE IF NOT EXISTS index_us_vix_daily (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  trade_date DATE NOT NULL COMMENT 'Trading date',
  open_value DECIMAL(12, 4) NULL COMMENT 'Open value',
  high_value DECIMAL(12, 4) NULL COMMENT 'High value',
  low_value DECIMAL(12, 4) NULL COMMENT 'Low value',
  close_value DECIMAL(12, 4) NULL COMMENT 'Close value',
  data_source VARCHAR(32) NOT NULL DEFAULT 'cboe_vix_history' COMMENT 'Data source',
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_index_us_vix_trade_date (trade_date),
  KEY idx_index_us_vix_trade_date (trade_date)
) COMMENT='Daily US VIX OHLC data';

CREATE TABLE IF NOT EXISTS index_us_fear_greed_daily (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  trade_date DATE NOT NULL COMMENT 'Trading date',
  fear_greed_value DECIMAL(8, 4) NULL COMMENT 'Fear and greed score',
  sentiment_label VARCHAR(32) NULL COMMENT 'Derived label from score',
  data_source VARCHAR(32) NOT NULL DEFAULT 'cnn_fear_greed_live' COMMENT 'Data source',
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_index_us_fear_greed_trade_date (trade_date),
  KEY idx_index_us_fear_greed_trade_date (trade_date)
) COMMENT='Daily US fear and greed index data';

CREATE TABLE IF NOT EXISTS index_us_hedge_fund_ls_proxy (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  report_date DATE NOT NULL COMMENT 'CFTC report date',
  contract_scope VARCHAR(8) NOT NULL COMMENT 'Proxy scope, e.g. ES or NQ',
  long_value DECIMAL(22, 2) NULL COMMENT 'Leveraged funds long notional value',
  short_value DECIMAL(22, 2) NULL COMMENT 'Leveraged funds short notional value',
  ratio_value DECIMAL(18, 6) NULL COMMENT 'Long short ratio',
  release_date DATE NULL COMMENT 'Estimated release date',
  data_source VARCHAR(32) NOT NULL DEFAULT 'ofr_tff' COMMENT 'Data source',
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_index_us_hedge_fund_ls_proxy_report_scope (report_date, contract_scope),
  KEY idx_index_us_hedge_fund_ls_proxy_report_date (report_date),
  KEY idx_index_us_hedge_fund_ls_proxy_contract_scope (contract_scope)
) COMMENT='US hedge fund long short proxy data from OFR TFF';

CREATE TABLE IF NOT EXISTS index_us_put_call_ratio_daily (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  trade_date DATE NOT NULL COMMENT 'Trading date',
  total_put_call_ratio DECIMAL(10, 4) NULL COMMENT 'Total put call ratio',
  index_put_call_ratio DECIMAL(10, 4) NULL COMMENT 'Index options put call ratio',
  equity_put_call_ratio DECIMAL(10, 4) NULL COMMENT 'Equity options put call ratio',
  etf_put_call_ratio DECIMAL(10, 4) NULL COMMENT 'ETF options put call ratio',
  data_source VARCHAR(64) NOT NULL DEFAULT 'cboe_market_statistics' COMMENT 'Data source',
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_index_us_put_call_ratio_trade_date (trade_date),
  KEY idx_index_us_put_call_ratio_trade_date (trade_date)
) COMMENT='Daily US options put call ratio from Cboe';

CREATE TABLE IF NOT EXISTS index_us_treasury_yield_daily (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  trade_date DATE NOT NULL COMMENT 'Trading date',
  yield_3m DECIMAL(10, 4) NULL COMMENT 'US Treasury 3 month yield',
  yield_2y DECIMAL(10, 4) NULL COMMENT 'US Treasury 2 year yield',
  yield_10y DECIMAL(10, 4) NULL COMMENT 'US Treasury 10 year yield',
  spread_10y_2y DECIMAL(10, 4) NULL COMMENT '10Y minus 2Y spread',
  spread_10y_3m DECIMAL(10, 4) NULL COMMENT '10Y minus 3M spread',
  data_source VARCHAR(64) NOT NULL DEFAULT 'fred_public_csv' COMMENT 'Data source',
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_index_us_treasury_yield_trade_date (trade_date),
  KEY idx_index_us_treasury_yield_trade_date (trade_date)
) COMMENT='Daily US Treasury yield and spread data from FRED';

CREATE TABLE IF NOT EXISTS index_us_credit_spread_daily (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  trade_date DATE NOT NULL COMMENT 'Trading date',
  high_yield_oas DECIMAL(10, 4) NULL COMMENT 'US high yield option-adjusted spread',
  data_source VARCHAR(64) NOT NULL DEFAULT 'fred_public_csv' COMMENT 'Data source',
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_index_us_credit_spread_trade_date (trade_date),
  KEY idx_index_us_credit_spread_trade_date (trade_date)
) COMMENT='Daily US high yield credit spread data from FRED';
