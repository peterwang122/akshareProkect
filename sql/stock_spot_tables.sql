-- 1) 新表：存放 stock_zh_a_spot_em 的代码和名称（查重建议依赖唯一索引）
CREATE TABLE IF NOT EXISTS stock_basic_info (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  stock_code VARCHAR(16) NOT NULL,
  stock_name VARCHAR(64) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_stock_code (stock_code)
);

-- 2) 扩充 stock_data，增加 4 个字段
ALTER TABLE stock_data
  ADD COLUMN pe_ttm DECIMAL(18,4) NULL COMMENT '市盈率-动态',
  ADD COLUMN pb DECIMAL(18,4) NULL COMMENT '市净率',
  ADD COLUMN total_market_value DECIMAL(24,2) NULL COMMENT '总市值',
  ADD COLUMN circulating_market_value DECIMAL(24,2) NULL COMMENT '流通市值';

-- 2.1) 扩充 stock_data，增加创建时间和更新时间
ALTER TABLE stock_data
  ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间';

-- 3) 建议索引（用于 date + stock_code 更新）
CREATE INDEX idx_stock_data_code_date ON stock_data(stock_code, date);
