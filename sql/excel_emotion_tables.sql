CREATE TABLE IF NOT EXISTS excel_index_emotion_daily (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  emotion_date DATE NOT NULL COMMENT '情绪指标日期',
  index_name VARCHAR(32) NOT NULL COMMENT '指数名称，如上证50、沪深300、中证500、中证1000',
  emotion_value DECIMAL(8, 2) NOT NULL COMMENT '情绪指标数值',
  source_file VARCHAR(255) NULL COMMENT '导入来源文件名',
  data_source VARCHAR(32) NOT NULL DEFAULT 'excel' COMMENT '数据来源',
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_emotion_date_index_name (emotion_date, index_name),
  KEY idx_index_name_date (index_name, emotion_date)
) COMMENT='Excel 导入的指数情绪指标日度数据';
