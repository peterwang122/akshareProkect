-- Run this script with the current session pinned to Asia/Shanghai (+08:00)
-- so existing TIMESTAMP values are materialized into local DATETIME values.
SET time_zone = '+08:00';

SET @stmt = (
  SELECT IF(
    EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = DATABASE() AND table_name = 'stock_info_all' AND column_name = 'created_at'
    ),
    "ALTER TABLE stock_info_all
       MODIFY created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
       MODIFY updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    "SELECT 1"
  )
);
PREPARE stmt FROM @stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @stmt = (
  SELECT IF(
    EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = DATABASE() AND table_name = 'stock_daily_data' AND column_name = 'created_at'
    ),
    "ALTER TABLE stock_daily_data
       MODIFY created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
       MODIFY updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    "SELECT 1"
  )
);
PREPARE stmt FROM @stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @stmt = (
  SELECT IF(
    EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = DATABASE() AND table_name = 'stock_qfq_daily_data' AND column_name = 'created_at'
    ),
    "ALTER TABLE stock_qfq_daily_data
       MODIFY created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
       MODIFY updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    "SELECT 1"
  )
);
PREPARE stmt FROM @stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @stmt = (
  SELECT IF(
    EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = DATABASE() AND table_name = 'stock_hfq_daily_data' AND column_name = 'created_at'
    ),
    "ALTER TABLE stock_hfq_daily_data
       MODIFY created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
       MODIFY updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    "SELECT 1"
  )
);
PREPARE stmt FROM @stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @stmt = (
  SELECT IF(
    EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = DATABASE() AND table_name = 'stock_basic_info' AND column_name = 'created_at'
    ),
    "ALTER TABLE stock_basic_info
       MODIFY created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
       MODIFY updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    "SELECT 1"
  )
);
PREPARE stmt FROM @stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @stmt = (
  SELECT IF(
    EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = DATABASE() AND table_name = 'stock_data' AND column_name = 'created_at'
    ),
    "ALTER TABLE stock_data
       MODIFY created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
       MODIFY updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'",
    "SELECT 1"
  )
);
PREPARE stmt FROM @stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @stmt = (
  SELECT IF(
    EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = DATABASE() AND table_name = 'index_basic_info' AND column_name = 'created_at'
    ),
    "ALTER TABLE index_basic_info
       MODIFY created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
       MODIFY updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    "SELECT 1"
  )
);
PREPARE stmt FROM @stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @stmt = (
  SELECT IF(
    EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = DATABASE() AND table_name = 'index_daily_data' AND column_name = 'created_at'
    ),
    "ALTER TABLE index_daily_data
       MODIFY created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
       MODIFY updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    "SELECT 1"
  )
);
PREPARE stmt FROM @stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @stmt = (
  SELECT IF(
    EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = DATABASE() AND table_name = 'quant_index_dashboard_daily' AND column_name = 'created_at'
    ),
    "ALTER TABLE quant_index_dashboard_daily
       MODIFY created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
       MODIFY updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    "SELECT 1"
  )
);
PREPARE stmt FROM @stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @stmt = (
  SELECT IF(
    EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = DATABASE() AND table_name = 'forex_basic_info' AND column_name = 'created_at'
    ),
    "ALTER TABLE forex_basic_info
       MODIFY created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
       MODIFY updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    "SELECT 1"
  )
);
PREPARE stmt FROM @stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @stmt = (
  SELECT IF(
    EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = DATABASE() AND table_name = 'forex_daily_data' AND column_name = 'created_at'
    ),
    "ALTER TABLE forex_daily_data
       MODIFY created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
       MODIFY updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    "SELECT 1"
  )
);
PREPARE stmt FROM @stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @stmt = (
  SELECT IF(
    EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = DATABASE() AND table_name = 'futures_daily_data' AND column_name = 'created_at'
    ),
    "ALTER TABLE futures_daily_data
       MODIFY created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
       MODIFY updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    "SELECT 1"
  )
);
PREPARE stmt FROM @stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @stmt = (
  SELECT IF(
    EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = DATABASE() AND table_name = 'option_cffex_spot_data' AND column_name = 'created_at'
    ),
    "ALTER TABLE option_cffex_spot_data
       MODIFY created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
       MODIFY updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    "SELECT 1"
  )
);
PREPARE stmt FROM @stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @stmt = (
  SELECT IF(
    EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = DATABASE() AND table_name = 'option_cffex_daily_data' AND column_name = 'created_at'
    ),
    "ALTER TABLE option_cffex_daily_data
       MODIFY created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
       MODIFY updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    "SELECT 1"
  )
);
PREPARE stmt FROM @stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @stmt = (
  SELECT IF(
    EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = DATABASE() AND table_name = 'option_cffex_rtj_daily_data' AND column_name = 'created_at'
    ),
    "ALTER TABLE option_cffex_rtj_daily_data
       MODIFY created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
       MODIFY updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    "SELECT 1"
  )
);
PREPARE stmt FROM @stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @stmt = (
  SELECT IF(
    EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = DATABASE() AND table_name = 'cffex_member_rankings' AND column_name = 'created_at'
    ),
    "ALTER TABLE cffex_member_rankings
       MODIFY created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
       MODIFY updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    "SELECT 1"
  )
);
PREPARE stmt FROM @stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @stmt = (
  SELECT IF(
    EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = DATABASE() AND table_name = 'douyin_index_emotion_daily' AND column_name = 'created_at'
    ),
    "ALTER TABLE douyin_index_emotion_daily
       MODIFY created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
       MODIFY updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    "SELECT 1"
  )
);
PREPARE stmt FROM @stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @stmt = (
  SELECT IF(
    EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = DATABASE() AND table_name = 'excel_index_emotion_daily' AND column_name = 'created_at'
    ),
    "ALTER TABLE excel_index_emotion_daily
       MODIFY created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
       MODIFY updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    "SELECT 1"
  )
);
PREPARE stmt FROM @stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @stmt = (
  SELECT IF(
    EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = DATABASE() AND table_name = 'etf_basic_info' AND column_name = 'created_at'
    ),
    "ALTER TABLE etf_basic_info
       MODIFY created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
       MODIFY updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    "SELECT 1"
  )
);
PREPARE stmt FROM @stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @stmt = (
  SELECT IF(
    EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = DATABASE() AND table_name = 'etf_daily_data' AND column_name = 'created_at'
    ),
    "ALTER TABLE etf_daily_data
       MODIFY created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
       MODIFY updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    "SELECT 1"
  )
);
PREPARE stmt FROM @stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @stmt = (
  SELECT IF(
    EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = DATABASE() AND table_name = 'etf_basic_info_sina' AND column_name = 'created_at'
    ),
    "ALTER TABLE etf_basic_info_sina
       MODIFY created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
       MODIFY updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    "SELECT 1"
  )
);
PREPARE stmt FROM @stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @stmt = (
  SELECT IF(
    EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = DATABASE() AND table_name = 'etf_daily_data_sina' AND column_name = 'created_at'
    ),
    "ALTER TABLE etf_daily_data_sina
       MODIFY created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
       MODIFY updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    "SELECT 1"
  )
);
PREPARE stmt FROM @stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @stmt = (
  SELECT IF(
    EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = DATABASE() AND table_name = 'daily_task_failures' AND column_name = 'created_at'
    ),
    "ALTER TABLE daily_task_failures
       MODIFY created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
       MODIFY updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    "SELECT 1"
  )
);
PREPARE stmt FROM @stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @stmt = (
  SELECT IF(
    EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = DATABASE() AND table_name = 'ak_request_jobs' AND column_name = 'created_at'
    ),
    "ALTER TABLE ak_request_jobs
       MODIFY created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
       MODIFY updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    "SELECT 1"
  )
);
PREPARE stmt FROM @stmt; EXECUTE stmt; DEALLOCATE PREPARE stmt;
