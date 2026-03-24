# 数据库展示对接说明

这份文档用于给另一个项目快速对接本项目数据库，重点说明：

- 每类数据对应哪张表
- 关键字段是什么
- 前端应该按什么维度展示
- 哪些表是业务展示表，哪些表更偏内部调度/运维

## 总览

| 模块 | 表名 | 主展示维度 |
|---|---|---|
| 股票基础信息 | `stock_basic_info` | `stock_code` |
| 股票行情 | `stock_data` | `stock_code + date` |
| 指数基础信息 | `index_basic_info` | `index_code` |
| 指数日线 | `index_daily_data` | `index_code + trade_date` |
| CFFEX 会员排名 | `cffex_member_rankings` | `product_code + contract_code + trade_date + rank_no` |
| 抖音情绪 | `douyin_index_emotion_daily` | `emotion_date` |
| 外汇基础信息 | `forex_basic_info` | `symbol_code` |
| 外汇日线 | `forex_daily_data` | `symbol_code + trade_date` |
| ETF 基础信息 | `etf_basic_info` | `etf_code` |
| ETF 日线/快照 | `etf_daily_data` | `etf_code + trade_date` |
| 中金所期货日线 | `futures_daily_data` | `symbol + trade_date + data_source` |
| 中金所期权链 | `option_cffex_spot_data` | `index_type + contract_symbol + strike_price` |
| 中金所期权日线 | `option_cffex_daily_data` | `option_symbol + trade_date` |
| Excel 情绪 | `excel_index_emotion_daily` | `emotion_date + index_name` |
| 失败任务 | `daily_task_failures` | `task_name + task_stage + task_key` |
| AK 请求调度 | `ak_request_jobs` | `id / request_key` |

## 1. 股票

### `stock_basic_info`

用途：

- 股票搜索
- 代码名称映射
- 下拉列表

核心字段：

- `stock_code`
- `stock_name`
- `created_at`
- `updated_at`

### `stock_data`

用途：

- 股票 K 线图
- 股票日行情表格
- 股票估值面板

核心字段：

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

推荐展示：

- 个股详情页
- K 线图
- 估值趋势

## 2. 指数

### `index_basic_info`

核心字段：

- `index_code`
- `simple_code`
- `market`
- `index_name`

### `index_daily_data`

核心字段：

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

推荐展示：

- 指数列表页
- 指数详情页
- 指数涨跌榜

## 3. CFFEX 会员排名

### `cffex_member_rankings`

核心字段：

- `product_code`
- `product_name`
- `contract_code`
- `trade_date`
- `rank_no`
- `volume_*`
- `long_*`
- `short_*`

推荐展示：

- 品种详情页
- 合约详情页
- 当日会员排名榜

## 4. 情绪指标

### `douyin_index_emotion_daily`

核心字段：

- `emotion_date`
- `hs300_emotion`
- `zz500_emotion`
- `zz1000_emotion`
- `sz50_emotion`
- `video_id`
- `video_url`
- `video_title`
- `extraction_status`

推荐展示：

- 情绪总览卡片
- 四大指数情绪趋势图

### `excel_index_emotion_daily`

核心字段：

- `emotion_date`
- `index_name`
- `emotion_value`
- `source_file`
- `data_source`

推荐展示：

- 稳定日频情绪图
- 日期维度对比表

说明：

- `excel_index_emotion_daily` 更适合稳定展示
- `douyin_index_emotion_daily` 更适合展示原始视频来源和自动提取结果

## 5. 外汇

### `forex_basic_info`

核心字段：

- `symbol_code`
- `symbol_name`

### `forex_daily_data`

核心字段：

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

推荐展示：

- 汇率详情页
- 美元指数与汇率联动图

## 6. ETF

### `etf_basic_info`

核心字段：

- `etf_code`
- `etf_name`
- `created_at`
- `updated_at`

### `etf_daily_data`

用途：

- ETF 历史前复权日线
- ETF 当日实时快照

核心字段：

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

- 通过 `data_source` 区分历史和快照
- `fund_etf_hist_em` + `adjust_type='qfq'` 适合历史图
- `fund_etf_spot_em` / `fund_etf_spot_ths` 适合当日快照

## 7. 中金所期货

### `futures_daily_data`

核心字段：

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
- `data_source='futures_hist_em'` 作为主连/当月连续专题页

## 8. 中金所期权

### `option_cffex_spot_data`

用途：

- 合约链
- 行权价表格

核心字段：

- `index_type`
- `index_name`
- `product_code`
- `contract_symbol`
- `strike_price`
- `call_option_symbol`
- `put_option_symbol`
- `call_*`
- `put_*`
- `data_source`

### `option_cffex_daily_data`

用途：

- 期权日线
- 指定期权历史走势

核心字段：

- `index_type`
- `index_name`
- `product_code`
- `contract_symbol`
- `option_symbol`
- `option_type`
- `strike_price`
- `trade_date`
- `open_price`
- `high_price`
- `low_price`
- `close_price`
- `volume`
- `data_source`

## 9. 失败任务

### `daily_task_failures`

用途：

- 日更失败重试
- 指定日期缺失修复
- ETF 历史补扫失败追踪

核心字段：

- `task_name`
- `task_stage`
- `task_key`
- `payload_json`
- `error_message`
- `result_status`
- `status`
- `retry_count`
- `first_failed_at`
- `last_failed_at`
- `resolved_at`

说明：

- 这是业务补采和失败恢复表
- 不是前端业务展示主表，但可以做运维后台页面

## 10. AK 请求调度

### `ak_request_jobs`

用途：

- 记录所有 AKShare 请求
- 承载独立调度服务的持久化队列
- 追踪请求级别的重试、父子依赖和结果状态

核心字段：

- `id`
- `request_key`
- `function_name`
- `source_group`
- `status`
- `attempt_count`
- `next_run_at`
- `lease_until`
- `parent_job_id`
- `root_job_id`
- `workflow_name`
- `caller_name`
- `error_category`
- `error_message`
- `result_type`
- `result_json`
- `created_at`
- `started_at`
- `finished_at`
- `updated_at`

展示建议：

- 这张表更适合做内部运维页面
- 不建议直接给普通业务前端展示
- 可以按 `source_group`、`status`、`function_name` 做调度监控

## 常见查询建议

### 股票详情页

- 从 `stock_basic_info` 拿名称
- 从 `stock_data` 按 `stock_code` + 日期范围查 K 线

### ETF 详情页

- 从 `etf_basic_info` 拿名称
- 历史图优先查 `etf_daily_data` 中 `data_source='fund_etf_hist_em'`
- 今日快照优先查当日最新一条 spot 数据

### 期权页面

- 合约链来自 `option_cffex_spot_data`
- 日线来自 `option_cffex_daily_data`

### 运维后台

- 失败恢复看 `daily_task_failures`
- AK 请求调度看 `ak_request_jobs`
