# 数据库展示对接说明

这份文档用于给另一个项目快速对接本项目数据库，重点说明：

- 每类数据对应哪张表
- 关键字段是什么
- 表之间怎么关联
- 前端适合如何展示

## 总览

| 模块 | 表名 | 主展示维度 |
|---|---|---|
| 股票基础信息 | `stock_basic_info` | 股票代码、股票名称 |
| 股票历史行情 | `stock_data` | `stock_code + date` |
| 指数基础信息 | `index_basic_info` | 指数代码、指数名称 |
| 指数日线 | `index_daily_data` | `index_code + trade_date` |
| CFFEX 会员排名 | `cffex_member_rankings` | `product_code + contract_code + trade_date + rank_no` |
| 抖音情绪 | `douyin_index_emotion_daily` | `emotion_date` |
| 外汇基础信息 | `forex_basic_info` | 汇率代码、名称 |
| 外汇日线 | `forex_daily_data` | `symbol_code + trade_date` |
| 中金所期货日线 | `futures_daily_data` | `symbol + trade_date` |
| 中金所期权链 | `option_cffex_spot_data` | `index_type + contract_symbol + strike_price` |
| 中金所期权日线 | `option_cffex_daily_data` | `option_symbol + trade_date` |
| Excel 情绪 | `excel_index_emotion_daily` | `emotion_date + index_name` |
| 失败任务 | `daily_task_failures` | 任务重试与状态追踪 |

## 1. 股票

### 1.1 `stock_basic_info`

用途：

- 股票下拉框
- 股票搜索
- 代码名称映射

关键字段：

| 字段 | 含义 |
|---|---|
| `stock_code` | 股票代码，唯一 |
| `stock_name` | 股票名称 |
| `created_at` | 创建时间 |
| `updated_at` | 更新时间 |

### 1.2 `stock_data`

用途：

- K 线图
- 行情表格
- 估值指标展示

关键字段：

| 字段 | 含义 |
|---|---|
| `stock_code` | 股票代码 |
| `date` | 交易日期 |
| `open_price` | 开盘价 |
| `close_price` | 收盘价 |
| `high_price` | 最高价 |
| `low_price` | 最低价 |
| `volume` | 成交量 |
| `turnover` | 成交额 |
| `amplitude` | 振幅 |
| `price_change_rate` | 涨跌幅 |
| `price_change_amount` | 涨跌额 |
| `turnover_rate` | 换手率 |
| `pe_ttm` | 市盈率-动态 |
| `pb` | 市净率 |
| `total_market_value` | 总市值 |
| `circulating_market_value` | 流通市值 |
| `created_at` | 创建时间 |
| `updated_at` | 更新时间 |

推荐展示：

- 股票详情页
- 日线 K 线页
- 股票估值面板

## 2. 指数

### 2.1 `index_basic_info`

关键字段：

- `index_code`
- `simple_code`
- `market`
- `index_name`

### 2.2 `index_daily_data`

关键字段：

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

推荐展示：

- 指数列表页
- 指数详情 K 线页
- 指数涨跌排行榜

## 3. CFFEX 会员排名

表名：

- `cffex_member_rankings`

关键字段：

| 字段 | 含义 |
|---|---|
| `product_code` | 品种代码，如 `IF`、`IH` |
| `product_name` | 品种名称 |
| `contract_code` | 合约代码，如 `IF1109` |
| `trade_date` | 交易日期 |
| `rank_no` | 排名 |
| `volume_*` | 成交量排名信息 |
| `long_*` | 持买单量排名信息 |
| `short_*` | 持卖单量排名信息 |

推荐展示：

- 品种详情页
- 合约详情页
- 当日会员排名榜

## 4. 抖音情绪指标

表名：

- `douyin_index_emotion_daily`

关键字段：

- `emotion_date`
- `hs300_emotion`
- `zz500_emotion`
- `zz1000_emotion`
- `sz50_emotion`
- `video_id`
- `video_url`
- `video_title`
- `raw_ocr_text`
- `extraction_status`

推荐展示：

- 情绪指标总览卡片
- 情绪日线趋势图
- 原始视频回溯链接

## 5. 外汇

### 5.1 `forex_basic_info`

关键字段：

- `symbol_code`
- `symbol_name`

### 5.2 `forex_daily_data`

关键字段：

- `symbol_code`
- `symbol_name`
- `trade_date`
- `open_price`
- `latest_price`
- `high_price`
- `low_price`
- `amplitude`
- `data_source`

`data_source` 常见值：

- `forex_hist_em`
- `forex_spot_em`
- `index_global_hist_em`

推荐展示：

- 汇率日线图
- 美元指数趋势图
- 汇率列表页

## 6. 中金所期货

表名：

- `futures_daily_data`

数据来源：

- `ak.get_futures_daily`
- `ak.futures_hist_em`

说明：

- 两条来源都会写入同一张 `futures_daily_data` 表
- 需要通过 `data_source` 区分数据来源后再展示
- `get_futures_daily` 主要是中金所常规期货日线
- `futures_hist_em` 主要是 8 个连续合约日线

`futures_hist_em` 当前固定采集的连续合约：

- `ICM`：中证500股指主连
- `ICM0`：中证500股指当月连续
- `IFM`：沪深主连
- `IFM0`：沪深当月连续
- `IHM`：上证主连
- `IHM0`：上证当月连续
- `IMM`：中证1000股指主连
- `IMM0`：中证1000股指当月连续

关键字段：

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

`data_source` 常见值：

- `get_futures_daily`
- `futures_hist_em`

前端展示建议：

- 如果做普通期货合约页，可优先展示 `data_source = 'get_futures_daily'`
- 如果做主连 / 当月连续专题页，可筛选 `data_source = 'futures_hist_em'`
- 同一 `symbol + trade_date` 是唯一键，列表页按 `symbol` 分组最直接

推荐展示：

- 期货合约日线图
- 成交量 / 持仓量图
- 合约列表页

## 7. 中金所期权

### 7.1 `option_cffex_spot_data`

用途：

- 期权链展示
- 行权价分布
- 认购 / 认沽对照

关键字段：

- `index_type`
- `index_name`
- `product_code`
- `contract_symbol`
- `strike_price`
- `call_option_symbol`
- `put_option_symbol`
- `call_latest_price`
- `put_latest_price`

### 7.2 `option_cffex_daily_data`

用途：

- 单个期权合约日线
- 期权历史成交走势

关键字段：

- `index_type`
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

推荐展示：

- 期权链页面
- 单个期权合约详情页

## 8. Excel 情绪指标

表名：

- `excel_index_emotion_daily`

关键字段：

- `emotion_date`
- `index_name`
- `emotion_value`
- `source_file`
- `data_source`

推荐展示：

- 情绪折线图
- 指数情绪对比图

和抖音情绪表的关系：

- `excel_index_emotion_daily` 更适合做稳定展示和人工维护数据源
- `douyin_index_emotion_daily` 更适合做自动抓取来源和追溯

## 9. 失败任务表

表名：

- `daily_task_failures`

主要用途：

- 保存日更或修复任务失败记录
- 记录任务最近一次结果是 `FAILED` 还是 `SUCCESS`
- 给手动补采脚本提供重试来源

关键字段：

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

## 常见前端查询建议

### 股票详情页

- 先查 `stock_basic_info`
- 再按 `stock_code` 查 `stock_data`

### 指数详情页

- 先查 `index_basic_info`
- 再按 `index_code` 查 `index_daily_data`

### 期权链页面

- 先查 `option_cffex_spot_data`
- 点击具体期权后再查 `option_cffex_daily_data`

### 情绪页面

- 稳定展示优先查 `excel_index_emotion_daily`
- 自动抓取回溯可补查 `douyin_index_emotion_daily`

### 汇率页面

- 汇率列表查 `forex_basic_info`
- 历史走势查 `forex_daily_data`

## SQL 文件位置

- [stock_spot_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/stock_spot_tables.sql)
- [index_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/index_tables.sql)
- [cffex_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/cffex_tables.sql)
- [douyin_emotion_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/douyin_emotion_tables.sql)
- [forex_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/forex_tables.sql)
- [futures_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/futures_tables.sql)
- [option_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/option_tables.sql)
- [excel_emotion_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/excel_emotion_tables.sql)
- [failed_task_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/failed_task_tables.sql)
