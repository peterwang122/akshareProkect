# 数据库展示说明

本文档给前端项目、接口项目和报表项目使用，帮助快速理解当前数据库里每类数据应该从哪张表读取、如何区分来源、哪些表已经停更。

## 总表
| 数据类型 | 表名 | 唯一键/核心键 | 备注 |
| --- | --- | --- | --- |
| 股票基础信息 | `stock_info_all` | `prefixed_code` | 新股票主表 |
| 股票日线/日快照 | `stock_daily_data` | `prefixed_code + trade_date` | 新股票主表 |
| 股票后复权日线 | `stock_hfq_daily_data` | `prefixed_code + trade_date` | 临时服务当前活动表 |
| 股票前复权日线 | `stock_qfq_daily_data` | `prefixed_code + trade_date` | 历史参考，不再供 FIT 使用 |
| A 股指数基础信息 | `index_basic_info` | `index_code` | 持续更新 |
| A 股指数日线 | `index_daily_data` | `index_code + trade_date` | 持续更新 |
| 美股指数基础信息 | `index_us_basic_info` | `index_code` | 持续更新 |
| 美股指数日线 | `index_us_daily_data` | `index_code + trade_date` | 持续更新 |
| 港股指数基础信息 | `index_hk_basic_info` | `index_code` | 持续更新 |
| 港股指数日线 | `index_hk_daily_data` | `index_code + trade_date` | 持续更新 |
| QVIX 指数基础信息 | `index_qvix_basic_info` | `index_code` | 持续更新 |
| QVIX 指数日线 | `index_qvix_daily_data` | `index_code + trade_date` | 持续更新 |
| 新闻情绪范围日线 | `index_news_sentiment_scope_daily` | `trade_date` | 持续更新 |
| 美股 VIX 日线 | `index_us_vix_daily` | `trade_date` | 持续更新 |
| 美股恐贪日线 | `index_us_fear_greed_daily` | `trade_date` | 持续更新 |
| 美股对冲基金多空代理 | `index_us_hedge_fund_ls_proxy` | `report_date + contract_scope` | 持续更新 |
| 量化指数看板预计算 | `quant_index_dashboard_daily` | `index_code + trade_date` | 持续更新 |
| CFFEX 会员排名 | `cffex_member_rankings` | `trade_date + product_code + contract_code + rank_no` | 持续更新 |
| 外汇基础信息 | `forex_basic_info` | `symbol_code` | 持续更新 |
| 外汇日线 | `forex_daily_data` | `symbol_code + trade_date` | 持续更新 |
| ETF 基础信息（新浪） | `etf_basic_info_sina` | `etf_code` | 新链路 |
| ETF 日线（新浪） | `etf_daily_data_sina` | `etf_code + trade_date` | 新链路 |
| 期货日线 | `futures_daily_data` | `symbol + trade_date + data_source` | 具体合约和连续共存 |
| 期权网页日统计 | `option_cffex_rtj_daily_data` | `contract_code + trade_date` | 新链路 |
| 抖音情绪指标 | `douyin_index_emotion_daily` | `emotion_date` | 持续更新 |
| Excel 情绪指标 | `excel_index_emotion_daily` | `emotion_date + index_name` | 手工导入 |
| 失败任务 | `daily_task_failures` | `task_name + task_stage + task_key` | 调度/回补辅助 |
| AK 调度请求 | `ak_request_jobs` | `id / request_key` | AK 调度服务内部表 |

## 股票
### `stock_info_all`
- 用途：股票代码、前缀代码、交易所、板块、上市日期以及多来源原始字段归档。
- 成分口径：
  - 上交所仅保留 `主板A股` 和 `科创板`
  - 深交所仅保留 `主板` 和 `创业板`
  - 北交所保留全量
- 同步规则：
  - 每天最多刷新一次
  - 若库中存在不属于上述口径的旧记录，下一次同步会强制重刷并删除这些多余记录
- 推荐展示：
  - 股票搜索框
  - 股票名称和代码映射
  - 证券基础资料页
- 核心字段：
  - `stock_code`：6 位纯代码
  - `prefixed_code`：`sh/sz/bj + stock_code`
  - `exchange`
  - `market_prefix`
  - `board`
  - `security_type`
  - `stock_name`
  - `security_full_name`
  - `company_abbr`
  - `company_full_name`
  - `list_date`
  - `industry`
  - `region`
  - `total_share_capital`
  - `circulating_share_capital`
  - `source_variants_json`
  - `raw_records_json`
- 说明：
  - 该表已经替代旧的 `stock_basic_info`
  - `source_variants_json` / `raw_records_json` 用于保留三交易所接口的全部来源字段

### `stock_daily_data`
- 用途：股票非复权日线和当天快照。
- 推荐展示：
  - 股票日 K
  - 当日行情表
  - 历史非复权走势
- 核心字段：
  - `stock_code`
  - `prefixed_code`
  - `stock_name`
  - `trade_date`
  - `open_price`
  - `close_price`
  - `high_price`
  - `low_price`
  - `latest_price`
  - `pre_close_price`
  - `buy_price`
  - `sell_price`
  - `price_change_amount`
  - `price_change_rate`
  - `volume`
  - `turnover_amount`
  - `data_source`
  - `snapshot_time`
- `data_source` 说明：
  - `stock_zh_a_spot`：当日快照
  - `stock_zh_a_hist_tx`：腾讯历史非复权日线
- 说明：
  - 该表已经替代旧的 `stock_data`
  - 若需要补历史覆盖范围，可运行 `python run.py stock repair-backfill`，它会以 `stock_info_all` 为准补齐还没有任何历史日线的股票
  - 若需要当天实时快照，请优先读取 `data_source='stock_zh_a_spot'`
  - 若需要历史非复权日线，请优先读取 `data_source='stock_zh_a_hist_tx'`

### `stock_hfq_daily_data`
- 用途：按单只股票临时采集的后复权日线。
- 推荐展示：
  - 后复权 K 线
  - 供 FIT 和其他项目按需刷新后读取
- 核心字段：
  - `stock_code`
  - `prefixed_code`
  - `stock_name`
  - `trade_date`
  - `open_price`
  - `close_price`
  - `high_price`
  - `low_price`
  - `price_change_amount`
  - `price_change_rate`
  - `volume`
  - `turnover_amount`
  - `outstanding_share`
  - `turnover_rate`
  - `data_source`
  - `request_start_date`
  - `request_end_date`
  - `refresh_batch_id`
- 说明：
  - 该表只由独立服务 `stock_temp_service.py` 写入
  - 当前临时服务调用 `stock_zh_a_daily(adjust="hfq")`
  - 同一股票如果请求区间变化，会删除旧数据后整表重写
  - 当前会在写库前按“前一交易日收盘价”自行计算：
    - `price_change_amount = 当日 close_price - 前一交易日 close_price`
    - `price_change_rate = price_change_amount / 前一交易日 close_price * 100`
  - 每次请求区间中的第一条记录因为没有区间内上一交易日收盘价，`price_change_amount` 和 `price_change_rate` 允许为 `NULL`

### `stock_qfq_daily_data`
- 用途：历史前复权参考表。
- 说明：
  - 该表保留历史数据，但 FIT 不再读取它
  - 当前临时服务已不再向该表写入新数据

## 指数
### `index_basic_info`
- 用途：A 股指数基础信息。
- 核心字段：
  - `index_code`
  - `simple_code`
  - `market`
  - `index_name`

### `index_daily_data`
- 用途：A 股指数日线和日快照。
- 核心字段：
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
- 说明：
  - 当前量化看板 `quant_index_dashboard_daily` 只读取这张 A 股指数表

### `index_us_basic_info`
- 用途：美股指数基础信息。
- 当前固定收录 4 个指数：
  - `.IXIC`
  - `.DJI`
  - `.INX`
  - `.NDX`

### `index_us_daily_data`
- 用途：美股指数历史和最新日线。
- 核心字段：
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
- 说明：
  - 数据源为 `index_us_stock_sina`
  - `amplitude`、`price_change_amount`、`price_change_rate` 按前一交易日收盘价本地计算
  - `turnover_rate` 固定为 `NULL`

### `index_hk_basic_info`
- 用途：港股指数基础信息。
- 说明：
  - 当前按 `stock_hk_index_spot_sina` 可返回的现货指数清单维护

### `index_hk_daily_data`
- 用途：港股指数历史和每日快照。
- 核心字段：
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
- 说明：
  - `data_source='stock_hk_index_spot_sina'` 表示港股指数每日快照
  - `data_source='stock_hk_index_daily_sina'` 表示港股指数历史回补
  - spot 链路直接使用接口返回的 `涨跌额`、`涨跌幅`
  - history 链路的 `amplitude`、`price_change_amount`、`price_change_rate` 按前一交易日收盘价本地计算
  - `turnover`、`turnover_rate` 固定为 `NULL`

### `index_qvix_basic_info`
- 用途：5 条固定 QVIX 序列的基础信息。
- 当前固定收录：
  - `50ETF_QVIX`
  - `300ETF_QVIX`
  - `500ETF_QVIX`
  - `CYB_QVIX`
  - `KCB_QVIX`

### `index_qvix_daily_data`
- 用途：QVIX 历史日线和每日最新值。
- 核心字段：
  - `index_code`
  - `trade_date`
  - `open_price`
  - `close_price`
  - `high_price`
  - `low_price`
  - `amplitude`
  - `price_change_rate`
  - `price_change_amount`
  - `data_source`
- 说明：
  - 对应 5 个 AK 接口：
    - `index_option_50etf_qvix`
    - `index_option_300etf_qvix`
    - `index_option_500etf_qvix`
    - `index_option_cyb_qvix`
    - `index_option_kcb_qvix`
  - `volume`、`turnover`、`turnover_rate` 默认是 `NULL`
  - `amplitude`、`price_change_amount`、`price_change_rate` 按前一交易日 `close` 本地计算

### `index_news_sentiment_scope_daily`
- 用途：新闻情绪范围时间序列。
- 核心字段：
  - `trade_date`
  - `sentiment_value`
  - `hs300_close`
  - `data_source`
- 说明：
  - 数据源是 `index_news_sentiment_scope`
  - 该接口当前只提供近一年可用数据
  - `backfill-news-sentiment` 的语义是“重拉当前可得近一年全量并 upsert”

### `index_us_vix_daily`
- 用途：美股 VIX 日线 OHLC。
- 核心字段：
  - `trade_date`
  - `open_value`
  - `high_value`
  - `low_value`
  - `close_value`
  - `data_source`
- 说明：
  - 当前数据源是 Cboe 官方 `VIX_History.csv`
  - 只接受完整 OHLC；如果某天源里缺少开高低收任一字段，则该日跳过不写
  - 这组数据通过 `python run.py index backfill-us-vix` / `python run.py index daily-us-vix` 维护

### `index_us_fear_greed_daily`
- 用途：美股恐贪指数日线。
- 核心字段：
  - `trade_date`
  - `fear_greed_value`
  - `sentiment_label`
  - `data_source`
- 说明：
  - 日更使用 CNN Fear & Greed 当前值
  - 历史回补使用固定公开历史镜像，并叠加 CNN 可追溯历史区间覆盖最近重叠数据
  - `sentiment_label` 固定按分值区间生成：`EXTREME_FEAR` / `FEAR` / `NEUTRAL` / `GREED` / `EXTREME_GREED`
  - 这组数据通过 `python run.py index backfill-us-fear-greed` / `python run.py index daily-us-fear-greed` 维护

### `index_us_hedge_fund_ls_proxy`
- 用途：美股对冲基金股票多空代理序列。
- 核心字段：
  - `report_date`
  - `contract_scope`
  - `long_value`
  - `short_value`
  - `ratio_value`
  - `release_date`
  - `data_source`
- 说明：
  - 当前固定存两条独立序列：
    - `ES`
    - `NQ`
  - 数据源是 OFR Hedge Fund Monitor / CFTC TFF 的 leveraged funds equity futures positioning
  - `ratio_value = long_value / short_value`
  - `release_date` 当前按 CFTC TFF 常规发布时间由 `report_date + 3 天` 推导
  - 该表按源的真实更新频率存储，不做日频补值，也不做 forward fill
  - 这组数据通过 `python run.py index backfill-us-hedge-proxy` / `python run.py index daily-us-hedge-proxy` 维护

### `quant_index_dashboard_daily`
- 用途：给 FIT 指数量化页直接读取的预计算表。
- 每个交易日固定 5 行：
  - 上证指数
  - 上证50
  - 沪深300
  - 中证500
  - 中证1000
- 核心字段：
  - `trade_date`
  - `index_code`
  - `index_name`
  - `emotion_value`
  - `main_basis`
  - `month_basis`
  - `breadth_up_count`
  - `breadth_total_count`
  - `breadth_up_pct`
- 口径说明：
  - 情绪值来自 `excel_index_emotion_daily`
  - 期现差来自 `futures_daily_data` 与 `index_daily_data`
  - 涨跌家数来自 `stock_daily_data`
  - 上证指数的情绪值和期现差按四大核心指数同日平均计算
  - 5 个指数在同一天共用同一份涨跌家数数据
  - 若股票数据补录或修正后需要整体重算，可执行 `python run.py quant-index refresh-breadth [start_date] [end_date]`

## CFFEX 会员排名
### `cffex_member_rankings`
- 用途：中金所会员排名、持仓变化分析。
- 核心字段：
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

## 外汇与美元指数
### `forex_basic_info`
- 核心字段：
  - `symbol_code`
  - `symbol_name`

### `forex_daily_data`
- 核心字段：
  - `symbol_code`
  - `symbol_name`
  - `trade_date`
  - `open_price`
  - `latest_price`
  - `high_price`
  - `low_price`
  - `amplitude`
  - `data_source`
- 说明：
  - 普通汇率和美元指数共用这张表

## ETF
### `etf_basic_info_sina`
- 用途：ETF 基础信息。
- 核心字段：
  - `etf_code`
  - `etf_name`
  - `sina_symbol`

### `etf_daily_data_sina`
- 用途：ETF 当日快照和历史日线。
- 核心字段：
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
- `data_source` 说明：
  - `fund_etf_category_sina`
  - `fund_etf_hist_sina`
- 说明：
  - 旧表 `etf_basic_info`、`etf_daily_data` 已停止更新

## 中金所期货
### `futures_daily_data`
- 用途：具体合约、主连、月连共用一张表。
- 核心字段：
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
- `data_source` 说明：
  - `get_futures_daily`：具体合约
  - `get_futures_daily_derived`：由具体合约派生的主连/月连
  - `futures_hist_em`：旧连续合约历史口径，保留但默认流程不再新增

## 外盘股指期货
### `futures_us_index_contract_info`
- 用途：美股股指期货连续/品种级合约注册表。
- 覆盖品种：`ES`、`NQ`。
- 数据源：新浪外盘期货。
- `source_contract_code`：固定为 `ES` / `NQ`。
- `contract_month`：固定为 `CONTINUOUS`，不表示具体月份合约。
- 旧库若已创建该表，需先执行 `sql/fix_us_index_futures_sina_source.sql` 放宽 `contract_month` 长度。
- 唯一键：`source_contract_code`。

### `futures_us_index_daily_data`
- 用途：美股股指期货连续/品种级日线。
- 唯一键：`source_contract_code + trade_date`。
- `open_price` / `high_price` / `low_price` / `close_price`：来自新浪日 K。
- `volume` / `open_interest` / `settle_price` / `pre_settle_price`：新浪返回为 `0` 时按 `NULL` 写入。
- `closing_range_raw`：新浪口径不使用，保持 `NULL`。
- `data_source`：`sina_global_futures`。

### `futures_hk_index_contract_info`
- 用途：港股股指期货具体月份合约注册表。
- 覆盖品种：`HSI`、`HHI`、`HTI`。
- 数据源：HKEX 官方 Daily Market Report。
- 唯一键：`source_contract_code`。

### `futures_hk_index_daily_data`
- 用途：港股股指期货具体月份合约日线。
- 唯一键：`source_contract_code + trade_date`。
- `close_price` 口径：使用 HKEX 报告中的 `Settlement Price`。
- `data_source`：`hkex_daily_market_report`。
- 回补说明：`backfill-hk-index` 会先按港股交易日生成待采集日期，并在执行中输出进度、耗时、ETA 和累计写入行数。
- 日更调度：FIT 任务中心使用 `us_index_futures_daily` 和 `hk_index_futures_daily` 两个独立采集项，分别调用 `POST /collect-us-index-futures-daily`、`POST /collect-hk-index-futures-daily`。

## 中金所期权
### `option_cffex_rtj_daily_data`
- 用途：中金所网页日统计期权数据。
- 核心字段：
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
- 说明：
  - 这是当前唯一持续更新的期权表
  - 旧 AK 期权表 `option_cffex_spot_data`、`option_cffex_daily_data` 保留但停止更新

## 情绪指标
### `douyin_index_emotion_daily`
- 用途：抖音视频解析后的指数情绪指标。
- 核心字段：
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
- 用途：Excel 导入的稳定版指数情绪指标。
- 核心字段：
  - `emotion_date`
  - `index_name`
  - `emotion_value`
  - `source_file`
  - `data_source`
- 说明：
  - 前端如果需要稳定展示情绪指标，优先使用这张表

## 调度与失败恢复
### `daily_task_failures`
- 用途：日更失败、专项补抓失败的数据库级追踪。
- 核心字段：
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

### `ak_request_jobs`
- 用途：AK scheduler 服务内部队列表。
- 核心字段：
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
  - `error_category`
  - `error_message`

## 旧表说明
以下表当前保留但已停止更新：

- `stock_basic_info`
- `stock_data`
- `etf_basic_info`
- `etf_daily_data`
- `option_cffex_spot_data`
- `option_cffex_daily_data`

如果要做新页面或新接口，请优先接入本文档中说明的新主表。  
## Quant Index Refresh Notes

- `quant_index_dashboard_daily` can be recalculated with:
  - `python run.py quant-index repair-recent [trade_day_count]`
  - `python run.py quant-index refresh-breadth [start_date] [end_date]`
- `excel_index_emotion_daily` now uses upsert semantics on `emotion_date + index_name`
- after `python run.py emotion-excel import [xlsx_path]`, the project automatically refreshes `quant_index_dashboard_daily` only for the affected trade dates
