# 数据库展示对接说明

这份文档用于给其他项目快速对接本项目数据库，重点说明：
- 每类数据在哪张表
- 关键字段是什么
- 前端/接口层应该按什么维度展示

## 总览

| 模块 | 表名 | 主展示维度 |
|---|---|---|
| 股票基础信息 | `stock_basic_info` | `stock_code` |
| 股票行情 | `stock_data` | `stock_code + date` |
| 指数基础信息 | `index_basic_info` | `index_code` |
| 指数日线 | `index_daily_data` | `index_code + trade_date` |
| CFFEX 会员排名 | `cffex_member_rankings` | `product_code + contract_code + trade_date + rank_no` |
| 抖音情绪 | `douyin_index_emotion_daily` | `emotion_date` |
| Excel 情绪 | `excel_index_emotion_daily` | `emotion_date + index_name` |
| 外汇基础信息 | `forex_basic_info` | `symbol_code` |
| 外汇日线 | `forex_daily_data` | `symbol_code + trade_date` |
| ETF 基础信息 | `etf_basic_info_sina` | `etf_code` |
| ETF 日线/快照 | `etf_daily_data_sina` | `etf_code + trade_date` |
| 中金所期货 | `futures_daily_data` | `symbol + trade_date + data_source` |
| 旧 AK 期权链 | `option_cffex_spot_data` / `option_cffex_daily_data` | 旧历史用途 |
| 新网页期权日统计 | `option_cffex_rtj_daily_data` | `contract_code + trade_date` |
| 失败任务 | `daily_task_failures` | `task_name + task_stage + task_key` |
| AK 调度请求 | `ak_request_jobs` | `id / request_key` |

## 股票

### `stock_basic_info`
- 用途：股票搜索、代码名称映射、下拉列表
- 关键字段：
  - `stock_code`
  - `stock_name`
  - `created_at`
  - `updated_at`

### `stock_data`
- 用途：个股 K 线、行情表、估值面板
- 关键字段：
  - `stock_code`
  - `date`
  - `open_price`
  - `close_price`
  - `high_price`
  - `low_price`
  - `volume`
  - `turnover`
  - `amplitude`
  - `price_change_rate`
  - `price_change_amount`
  - `turnover_rate`
  - `pe_ttm`
  - `pb`
  - `total_market_value`
  - `circulating_market_value`
  - `created_at`
  - `updated_at`

## 指数

### `index_basic_info`
- 关键字段：
  - `index_code`
  - `simple_code`
  - `market`
  - `index_name`

### `index_daily_data`
- 关键字段：
  - `index_code`
  - `trade_date`
  - `open_price`
  - `close_price`
  - `high_price`
  - `low_price`
  - `volume`
  - `turnover`
  - `amplitude`
  - `price_change_rate`
  - `price_change_amount`
  - `turnover_rate`
  - `data_source`

## CFFEX 会员排名

### `cffex_member_rankings`
- 用途：期货会员排名榜、持仓分析
- 关键字段：
  - `product_code`
  - `product_name`
  - `contract_code`
  - `trade_date`
  - `rank_no`
  - `volume_rank`
  - `volume_member`
  - `volume_value`
  - `volume_change_value`
  - `long_rank`
  - `long_member`
  - `long_open_interest`
  - `long_change_value`
  - `short_rank`
  - `short_member`
  - `short_open_interest`
  - `short_change_value`

## 情绪指标

### `douyin_index_emotion_daily`
- 用途：视频来源情绪指标
- 关键字段：
  - `emotion_date`
  - `hs300_emotion`
  - `zz500_emotion`
  - `zz1000_emotion`
  - `sz50_emotion`
  - `video_id`
  - `video_url`
  - `video_title`
  - `extraction_status`

### `excel_index_emotion_daily`
- 用途：稳定展示口径
- 关键字段：
  - `emotion_date`
  - `index_name`
  - `emotion_value`
  - `source_file`
  - `data_source`

## 外汇

### `forex_basic_info`
- 关键字段：
  - `symbol_code`
  - `symbol_name`

### `forex_daily_data`
- 用途：汇率和美元指数日线
- 关键字段：
  - `symbol_code`
  - `symbol_name`
  - `trade_date`
  - `open_price`
  - `latest_price`
  - `high_price`
  - `low_price`
  - `amplitude`
  - `data_source`
  - `created_at`
  - `updated_at`

## ETF

### `etf_basic_info`
- 关键字段：
  - `etf_code`
  - `etf_name`
  - `created_at`
  - `updated_at`

### `etf_daily_data`
- 用途：ETF 历史前复权和当日快照
- 关键字段：
  - `etf_code`
  - `etf_name`
  - `trade_date`
  - `open_price`
  - `close_price`
  - `high_price`
  - `low_price`
  - `volume`
  - `turnover`
  - `amplitude`
  - `price_change_rate`
  - `price_change_amount`
  - `turnover_rate`
  - `data_source`
  - `adjust_type`
  - `spot_data_date`
  - `spot_update_time`
  - `created_at`
  - `updated_at`

展示建议：
- `fund_etf_hist_em + adjust_type='qfq'` 适合历史图表
- `fund_etf_spot_em / fund_etf_spot_ths` 适合当日快照

## 中金所期货

当前正式 ETF 新链路：

### `etf_basic_info_sina`
- 用途：ETF 搜索、代码名称映射、保存新浪完整代码
- 关键字段：
  - `etf_code`
  - `etf_name`
  - `sina_symbol`
  - `created_at`
  - `updated_at`

### `etf_daily_data_sina`
- 用途：ETF 历史日线和当日快照
- 关键字段：
  - `etf_code`
  - `etf_name`
  - `sina_symbol`
  - `trade_date`
  - `open_price`
  - `close_price`
  - `high_price`
  - `low_price`
  - `volume`
  - `turnover`
  - `amplitude`
  - `price_change_rate`
  - `price_change_amount`
  - `turnover_rate`
  - `pre_close_price`
  - `data_source`
  - `created_at`
  - `updated_at`

展示建议：
- `fund_etf_hist_sina` 适合历史图表
- `fund_etf_category_sina` 适合当日快照
- 新链路统一读取 `etf_basic_info_sina` / `etf_daily_data_sina`
- 旧表 `etf_basic_info` / `etf_daily_data` 保留旧数据，但不再更新

### `futures_daily_data`
- 关键字段：
  - `market`
  - `symbol`
  - `variety`
  - `trade_date`
  - `open_price`
  - `high_price`
  - `low_price`
  - `close_price`
  - `volume`
  - `open_interest`
  - `turnover`
  - `settle_price`
  - `pre_settle_price`
  - `data_source`

展示建议：
- `data_source='get_futures_daily'` 作为普通期货日线
- `data_source='get_futures_daily_derived'` 作为默认主连/当月连续专题页
- `data_source='futures_hist_em'` 作为旧连续数据参考
- 项目默认 `futures backfill` / `futures daily` 现在生成的是 `get_futures_daily` 与 `get_futures_daily_derived`

连续合约派生规则：
- 只对 `IF / IC / IH / IM` 派生连续数据
- 主连：
  - `IFM / ICM / IHM / IMM`
  - 取同日同品种成交量最大的具体合约
- 月连：
  - `IFM0 / ICM0 / IHM0 / IMM0`
  - 取同日同品种最近到期的具体合约
  - 比较依据是完整 `YYMM`，可正确处理跨年

## 中金所期权

### 旧表：`option_cffex_spot_data`
- 来源：旧 AKShare `spot_sina`
- 状态：保留历史数据，不再更新

### 旧表：`option_cffex_daily_data`
- 来源：旧 AKShare `daily_sina`
- 状态：保留历史数据，不再更新

### 新表：`option_cffex_rtj_daily_data`
- 来源：中金所日统计页面 [http://www.cffex.com.cn/rtj/](http://www.cffex.com.cn/rtj/)
- 状态：当前正式期权主展示表
- 关键字段：
  - `index_type`
  - `index_name`
  - `product_prefix`
  - `contract_code`
  - `contract_month`
  - `option_type`
  - `strike_price`
  - `trade_date`
  - `open_price`
  - `high_price`
  - `low_price`
  - `close_price`
  - `settle_price`
  - `pre_settle_price`
  - `price_change_close`
  - `price_change_settle`
  - `volume`
  - `turnover`
  - `open_interest`
  - `open_interest_change`
  - `data_source`
  - `source_url`
  - `created_at`
  - `updated_at`

解析规则：
- `IO` -> `HS300`
- `MO` -> `ZZ1000`
- `HO` -> `SZ50`
- `-C-` -> `CALL`
- `-P-` -> `PUT`

展示建议：
- 前端新的期权页面统一读取 `option_cffex_rtj_daily_data`
- 不再从旧 AK 期权表取新数据
- 如果历史回补时少数日期失败，可以用 `python run.py option repair-backfill` 按数据库缺失交易日重新抓取

## Excel / 调度 / 运维表

### `daily_task_failures`
- 用途：顶层任务失败记录和人工重试
- 关键字段：
  - `task_name`
  - `task_stage`
  - `task_key`
  - `payload_json`
  - `error_message`
  - `status`
  - `result_status`
  - `retry_count`

### `ak_request_jobs`
- 用途：AK 调度服务请求队列
- 关键字段：
  - `request_key`
  - `function_name`
  - `source_group`
  - `status`
  - `attempt_count`
  - `next_run_at`
  - `parent_job_id`
  - `root_job_id`
  - `workflow_name`
  - `caller_name`

## 时间字段说明

项目当前的业务表元数据时间统一采用：
- `DATETIME`
- 数据库 session 固定 `+08:00`

因此当前新表的 `created_at / updated_at` 不应再出现 8 小时时差问题。
