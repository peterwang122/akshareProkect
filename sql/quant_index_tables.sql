CREATE TABLE IF NOT EXISTS quant_index_dashboard_daily (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  trade_date DATE NOT NULL COMMENT '交易日期',
  index_code VARCHAR(32) NOT NULL COMMENT '指数代码，需与 FIT /stocks/indexes/options 对齐',
  index_name VARCHAR(64) NOT NULL COMMENT '指数名称',
  emotion_value DECIMAL(18, 6) NOT NULL DEFAULT 50 COMMENT '情绪值',
  main_basis DECIMAL(18, 6) NOT NULL DEFAULT 0 COMMENT '主连期现差',
  month_basis DECIMAL(18, 6) NOT NULL DEFAULT 0 COMMENT '当月连续期现差',
  breadth_up_count INT NOT NULL DEFAULT 0 COMMENT '上涨家数',
  breadth_total_count INT NOT NULL DEFAULT 0 COMMENT '有效总家数',
  breadth_up_pct DECIMAL(18, 6) NOT NULL DEFAULT 0 COMMENT '上涨家数百分比',
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_quant_index_dashboard_daily_code_date (index_code, trade_date),
  KEY idx_quant_index_dashboard_daily_trade_date (trade_date),
  KEY idx_quant_index_dashboard_daily_name_date (index_name, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='量化指数看板预计算结果表';
