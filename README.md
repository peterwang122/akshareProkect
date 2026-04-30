# 数据采集项目说明

## 项目概览
本项目用于采集、回补、修复并维护多条金融数据链路，当前覆盖：

- 股票
- 指数
- 量化指数看板
- 中金所会员持仓排名
- 汇率与美元指数
- ETF
- 中金所期货
- 中金所期权日统计网页
- 抖音情绪指标
- Excel 情绪指标导入

当前正式入口分为两类：

- 统一采集入口：`python run.py <domain> <command> [args]`
- 独立服务入口：
  - `python ak_scheduler_service.py serve`
  - `python stock_temp_service.py serve`

## 运行前准备

### 1. 安装依赖
```bash
pip install -r requirements.txt
playwright install chromium
```

### 2. 数据库配置
数据库配置文件：

- `config/db_info.json`

建议在配置中显式设置：

```json
{
  "timezone": "+08:00"
}
```

项目中的新表时间字段统一使用 `DATETIME`，数据库连接会固定执行：

```sql
SET time_zone = '+08:00'
```

这样可以避免历史上出现过的 8 小时时差问题。

### 3. 运行期目录
所有日志、状态、缓存和运行产物统一收口到：

- `runtime/logs/`
- `runtime/state/`
- `runtime/cache/`
- `runtime/artifacts/`

## 启动顺序
如果要运行任何依赖 AKShare 的采集任务，推荐顺序如下：

1. 启动 AK 调度服务：`python ak_scheduler_service.py serve`
2. 如需接收另一个项目的股票后复权或 US/HK 指数日常采集请求，再启动：`python stock_temp_service.py serve`
3. 执行 `python run.py ...` 对应采集命令

以下网页抓取链路不依赖 AK 调度服务：

- `python run.py cffex ...`
- `python run.py option ...`
- `python run.py douyin ...`

## 命令总览

### 调度与服务
```bash
python ak_scheduler_service.py serve
python ak_scheduler_service.py health
python ak_scheduler_service.py doctor
python ak_scheduler_service.py doctor 50

python stock_temp_service.py serve
python stock_temp_service.py health
```

常用可复制命令：

```bash
python ak_scheduler_service.py serve
python ak_scheduler_service.py health
python ak_scheduler_service.py doctor 20
python stock_temp_service.py serve
python stock_temp_service.py health
```

### 统一采集入口
```bash
python run.py stock daily [stock_code ...]
python run.py stock backfill [stock_code ...]
python run.py stock repair-backfill [stock_code ...]
python run.py stock repair-daily-dates <YYYY-MM-DD> [YYYY-MM-DD ...] [--codes <stock_code ...>]
python run.py stock repair-hist-metrics [start_date] [end_date] [--codes <stock_code ...>]

python run.py index daily
python run.py index backfill
python run.py index backfill-bj899050
python run.py index backfill-us
python run.py index backfill-hk
python run.py index backfill-qvix
python run.py index daily-qvix
python run.py index backfill-news-sentiment
python run.py index daily-news-sentiment
python run.py index backfill-us-vix
python run.py index daily-us-vix
python run.py index backfill-us-fear-greed
python run.py index daily-us-fear-greed
python run.py index backfill-us-hedge-proxy
python run.py index daily-us-hedge-proxy
python run.py index backfill-us-market-sentiment
python run.py index daily-us-market-sentiment

python run.py quant-index daily [trade_date]
python run.py quant-index backfill [start_date] [end_date]
python run.py quant-index refresh-breadth [start_date] [end_date]
python run.py quant-index repair-recent [trade_day_count]

python run.py cffex daily [PRODUCT_CODE ...]
python run.py cffex backfill [PRODUCT_CODE ...]
python run.py cffex single <YYYY-MM-DD> <PRODUCT_CODE>

python run.py forex daily [SYMBOL ...]
python run.py forex backfill [SYMBOL ...]
python run.py forex repair-history [SYMBOL ...]
python run.py forex usd-backfill
python run.py forex usd-daily
python run.py forex usd-once

python run.py futures daily [trade_date|start_date end_date]
python run.py futures backfill [start_date] [end_date]
python run.py futures trade-date <YYYY-MM-DD>
python run.py futures market-daily [trade_date|start_date end_date]
python run.py futures market-backfill [start_date] [end_date]
python run.py futures hist-daily [trade_date|start_date end_date] [HIST_SYMBOL ...]
python run.py futures hist-backfill [start_date] [end_date] [HIST_SYMBOL ...]

python run.py etf daily [ETF_CODE ...]
python run.py etf backfill [ETF_CODE ...]
python run.py etf weekly-repair [ETF_CODE ...]
python run.py etf repair-backfill [ETF_CODE ...]

python run.py option daily [target_date]
python run.py option backfill [end_date]
python run.py option repair-backfill [date|start_date end_date]

python run.py douyin daily
python run.py douyin backfill

python run.py emotion-excel import [xlsx_path]

python run.py runner daily
python run.py runner retry-failures
python run.py runner retry-failures <task_name> <limit>
```

推荐启动顺序示例：

```bash
python ak_scheduler_service.py serve
python stock_temp_service.py serve
python run.py runner daily
```

## 调度服务
AKShare 请求统一走独立调度服务，不允许业务采集代码直接绕过调度器去请求 AK。

启动与自检：

```bash
python ak_scheduler_service.py serve
python ak_scheduler_service.py health
python ak_scheduler_service.py doctor
```

当前调度规则：

- 调度队列现在按真实上游来源拆分，不再只固定为 `eastmoney / sina / ths`
- 当前已启用的来源组包括：`eastmoney`、`sina`、`ths`、`tencent`、`optbbs`、`chinascope`、`sse`、`szse`、`bse`
- 各来源组彼此独立，可以并发，默认也各自独立节流
- 默认节流、重试、熔断开关等由 `config/ak_scheduler.json` 的 `source_policies` 控制
- `/health` 会返回当前调度服务版本、PID、启动时间和动态来源组状态
- `doctor` 用于排查 scheduler 是否还在产生空成功、旧进程未退出等问题

当前已经接入调度服务的主要 AK 函数来源：

- 东方财富：`stock_zh_a_spot`、`stock_zh_index_daily_em`、`index_zh_a_hist`、`forex_hist_em`、`index_global_hist_em`、`get_futures_daily` 等
- 新浪：`stock_zh_index_spot_sina`、`stock_zh_index_daily`、`index_us_stock_sina`、`stock_hk_index_spot_sina`、`stock_hk_index_daily_sina`、`fund_etf_category_sina`、`fund_etf_hist_sina`、`option_cffex_*_sina` 等
- 腾讯：`stock_zh_a_hist_tx`
- OptBBS：`index_option_50etf_qvix`、`index_option_300etf_qvix`、`index_option_500etf_qvix`、`index_option_cyb_qvix`、`index_option_kcb_qvix`
- Chinascope：`index_news_sentiment_scope`
- 交易所清单：`stock_info_sh_name_code` -> `sse`，`stock_info_sz_name_code` -> `szse`，`stock_info_bj_name_code` -> `bse`
- 同花顺：当前主要保留注册能力，具体是否使用以 collector 实现为准

给新接口选择 `source_group` 时，不要再按函数名猜来源；请先核对 AKShare 实际上游站点，再同时更新：

- `src/akshare_project/scheduler/registry.py`
- `config/ak_scheduler.json`

如果某个接口被分错组，最常见的后果是：

- 限流节奏落错队列，导致请求看起来“无故变慢”
- 某个上游的失败、重试或熔断状态污染到并不相关的接口

可直接复制的排查命令：

```bash
python ak_scheduler_service.py health
python ak_scheduler_service.py doctor
python ak_scheduler_service.py doctor 50
```

## 模块说明

### 股票
用途：

- 维护统一股票信息表
- 维护非复权日线 / 日快照
- 提供供外部项目调用的后复权临时采集服务

实际接口：

- `ak.stock_info_sh_name_code`
- `ak.stock_info_sz_name_code`
- `ak.stock_info_bj_name_code`
- `ak.stock_zh_a_spot`
- `ak.stock_zh_a_hist_tx`
- `ak.stock_zh_a_daily(adjust="hfq")`

写入表：

- `stock_info_all`
- `stock_daily_data`
- `stock_hfq_daily_data`
- `stock_qfq_daily_data`（历史参考，不再供 FIT 使用）

股票信息表当前成分口径：

- 上交所：
  - `stock_info_sh_name_code("主板A股")`
  - `stock_info_sh_name_code("科创板")`
- 深交所：
  - `stock_info_sz_name_code("A股列表")`
  - 只保留主板和创业板
- 北交所：
  - `stock_info_bj_name_code()`

关键规则：

- `stock_info_all` 每天最多自动刷新一次
- 如果表中存在超出口径的旧股票，哪怕今天已刷新过，也会强制重刷并删除多余记录
- 股票编码统一同时保存：
  - `stock_code`：6 位纯代码
  - `prefixed_code`：`sh/sz/bj + 6位代码`

命令说明：

- `python run.py stock daily [stock_code ...]`
  - 先同步 `stock_info_all`
  - 再调用 `stock_zh_a_spot`
  - 只写当前 `stock_info_all` 成分内的股票到 `stock_daily_data`
  - 日常采集只使用 `stock_zh_a_spot` 返回的整表快照写库
  - 不会在 `daily` 链路中逐股调用 `stock_zh_a_hist_tx`
  - 完成后会刷新受影响日期的 `quant_index_dashboard_daily`
- `python run.py stock backfill [stock_code ...]`
  - 先同步 `stock_info_all`
  - 再用 `stock_zh_a_spot` 获取当前股票池
  - 对每只股票按上市日期回补 `stock_zh_a_hist_tx`
  - 查不到上市日期时，从 `1991-01-01` 开始
- `python run.py stock repair-backfill [stock_code ...]`
  - 对照 `stock_info_all` 扫描“还没有任何 `stock_zh_a_hist_tx` 历史行”的股票
  - 只补缺失股票，不重刷全市场
  - 空历史结果按失败处理，不再静默跳过
- `python run.py stock repair-daily-dates <YYYY-MM-DD> [YYYY-MM-DD ...] [--codes <stock_code ...>]`
  - 用 `stock_zh_a_hist_tx` 覆盖修复指定交易日的错误日线
  - 可配合 `--codes` 只重跑少量失败股票
- `python run.py stock repair-hist-metrics [start_date] [end_date] [--codes <stock_code ...>]`
   - 只修 `stock_daily_data` 中 `data_source='stock_zh_a_hist_tx'` 的历史行
   - 纯数据库内补算 `pre_close_price`、`price_change_amount`、`price_change_rate`
   - 不重新请求 AK，不走 scheduler，也不会自动刷新 `quant_index_dashboard_daily`

可直接复制的常用命令：

```bash
python run.py stock daily
python run.py stock daily 600000 000001 430047
python run.py stock backfill
python run.py stock backfill 600000 000001
python run.py stock repair-backfill
python run.py stock repair-backfill 600000 000001
python run.py stock repair-daily-dates 2026-03-30 2026-03-31 2026-04-02 2026-04-03
python run.py stock repair-daily-dates 2026-03-30 2026-03-31 2026-04-02 2026-04-03 --codes 600735 603933 603950 000959 301309
python run.py stock repair-hist-metrics
python run.py stock repair-hist-metrics 2026-01-01 2026-03-31
python run.py stock repair-hist-metrics --codes 600000 000001
python run.py stock repair-hist-metrics 2026-01-01 2026-03-31 --codes 600000 000001
```

### 股票后复权临时采集服务
用途：

- 提供给另一个项目按股票代码触发后复权数据刷新
- 提供给另一个项目触发 US/HK 指数日常采集

服务命令：

```bash
python stock_temp_service.py serve
python stock_temp_service.py health
```

HTTP 接口：

- `GET /health`
- `POST /collect`
- `POST /collect-index-us-daily`
- `POST /collect-index-hk-daily`

请求示例：

```json
{
  "stock_code": "600000",
  "start_date": "2020-01-01",
  "end_date": "2026-03-28"
}
```

处理规则：

- 用 `stock_code` 到 `stock_info_all` 查上市日期
- 查不到上市日期时用 `1991-01-01`
- 调用 `stock_zh_a_daily(adjust="hfq")`
- 若请求区间与最近一次该股票区间完全一致，则返回 `UNCHANGED`
- 若区间发生变化，则先删除该股票在 `stock_hfq_daily_data` 的全部旧数据，再整批重写
- 后复权结果会在写库前按前一交易日收盘价自行计算：
  - `price_change_amount`
  - `price_change_rate`

说明：

- FIT 现在读取 `stock_hfq_daily_data`
- `stock_qfq_daily_data` 仅保留历史参考，不再作为活动读取表
- US/HK 指数日常采集也通过这套服务暴露，但不会复用股票 `/collect`

对接文档：

- `docs/STOCK_TEMP_SERVICE_INTEGRATION.md`
- `docs/DAILY_COLLECTION_SERVICE_INTEGRATION.md`
- `docs/INDEX_TEMP_SERVICE_INTEGRATION.md`

可直接复制的常用命令：

```bash
python stock_temp_service.py serve
python stock_temp_service.py health
```

可直接复制的 HTTP 请求示例：

```powershell
Invoke-RestMethod -Method Post `
  -Uri "http://127.0.0.1:8786/collect" `
  -ContentType "application/json" `
  -Body '{"stock_code":"600000","start_date":"2020-01-01","end_date":"2026-04-13"}'
```

```powershell
Invoke-RestMethod -Method Post `
  -Uri "http://127.0.0.1:8786/collect-index-us-daily" `
  -ContentType "application/json" `
  -Body '{}'
```

```powershell
Invoke-RestMethod -Method Post `
  -Uri "http://127.0.0.1:8786/collect-index-hk-daily" `
  -ContentType "application/json" `
  -Body '{}'
```

### 指数
用途：

- 维护指数基础信息
- 维护指数日线和日快照
- 维护 US/HK 指数历史和日常采集

实际接口：

- `ak.stock_zh_index_spot_sina`
- `ak.index_zh_a_hist`
- `ak.stock_zh_index_daily_em`
- `ak.stock_zh_index_daily`（仅 `bj899050` / 北证50 特例）
- `ak.index_us_stock_sina`
- `ak.stock_hk_index_spot_sina`
- `ak.stock_hk_index_daily_sina`

写入表：

- `index_basic_info`
- `index_daily_data`
- `index_us_basic_info`
- `index_us_daily_data`
- `index_hk_basic_info`
- `index_hk_daily_data`

命令说明：

- `python run.py index daily`
  - 从 `stock_zh_index_spot_sina` 抓取快照
  - 更新 `index_basic_info`
  - 更新 `index_daily_data`
  - 对 `bj899050`（北证50）会额外调用一次 `stock_zh_index_daily`
  - `bj899050` 的 `amplitude`、`price_change_amount`、`price_change_rate` 按前一交易日收盘价本地计算
- `python run.py index backfill`
  - 先抓指数快照拿到指数清单
  - 历史优先走 `index_zh_a_hist`
  - 若失败或无数据，再回退 `stock_zh_index_daily_em`
- `python run.py index backfill-bj899050`
  - 单独回补 `bj899050`（北证50）全历史
  - 直接使用 `stock_zh_index_daily`
  - `amplitude`、`price_change_amount`、`price_change_rate` 按前一交易日收盘价本地计算
- `python run.py index backfill-us`
  - 回补 4 个固定美股指数全历史：`.IXIC`、`.DJI`、`.INX`、`.NDX`
  - 使用 `index_us_stock_sina`
  - `amplitude`、`price_change_amount`、`price_change_rate` 按前一交易日收盘价本地计算
- `python run.py index backfill-hk`
  - 先通过 `stock_hk_index_spot_sina` 拿当前港股指数清单
  - 再逐个用 `stock_hk_index_daily_sina` 回补历史
  - 历史链路的 `amplitude`、`price_change_amount`、`price_change_rate` 按前一交易日收盘价本地计算
  - `stock_hk_index_daily_sina` 本身没有 `start_date` 参数，当前实现会一次写入该 symbol 的全部可用历史
  - 港股历史最早日期以新浪该指数实际能返回的最早交易日为准

US/HK 日常采集说明：

- US 日常采集不通过 `run.py index`，而是通过 `stock_temp_service.py serve` 的 `/collect-index-us-daily`
- HK 日常采集不通过 `run.py index`，而是通过 `stock_temp_service.py serve` 的 `/collect-index-hk-daily`
- HK spot 日常采集使用 `stock_hk_index_spot_sina`，`trade_date` 取本地当天日期，建议在收盘后调用
- US 日常采集使用 `index_us_stock_sina`，写入的是每个指数最新可得交易日
- 量化看板 `quant_index_dashboard_daily` 仍然只使用 A 股指数，不纳入 US/HK

可直接复制的常用命令：

```bash
python run.py index daily
python run.py index backfill
python run.py index backfill-bj899050
python run.py index backfill-us
python run.py index backfill-hk
python run.py index backfill-qvix
python run.py index daily-qvix
python run.py index backfill-news-sentiment
python run.py index daily-news-sentiment
python run.py index backfill-us-vix
python run.py index daily-us-vix
python run.py index backfill-us-fear-greed
python run.py index daily-us-fear-greed
python run.py index backfill-us-hedge-proxy
python run.py index daily-us-hedge-proxy
python run.py index backfill-us-market-sentiment
python run.py index daily-us-market-sentiment
```

美股风险情绪指标说明：

- `backfill-us-vix` / `daily-us-vix` 不通过 `stock_temp_service.py serve`
- `backfill-us-fear-greed` / `daily-us-fear-greed` 不通过 `stock_temp_service.py serve`
- `backfill-us-hedge-proxy` / `daily-us-hedge-proxy` 不通过 `stock_temp_service.py serve`
- `index_us_vix_daily` 只存 Cboe VIX 的 OHLC；如果某天拿不到完整开高低收，就会跳过该日
- `index_us_fear_greed_daily` 日更使用 CNN 当前值，历史回补使用固定公开镜像，并用 CNN 可追溯区间覆盖最近重叠数据
- `index_us_hedge_fund_ls_proxy` 当前保存两条独立代理序列：`ES` 和 `NQ`
- `backfill-us-market-sentiment` / `daily-us-market-sentiment` 继续保留为兼容命令，会顺序执行这 3 组数据

### 量化指数看板
用途：

- 生成 FIT 项目用的量化指数看板日表

依赖数据源：

- `index_daily_data`
- `excel_index_emotion_daily`
- `futures_daily_data`
- `stock_daily_data`

写入表：

- `quant_index_dashboard_daily`

当前固定产出 5 个指数：

- 上证指数
- 上证50
- 沪深300
- 中证500
- 中证1000

命令说明：

- `python run.py quant-index daily [trade_date]`
  - 计算并覆盖某一天的看板数据
- `python run.py quant-index backfill [start_date] [end_date]`
  - 批量回补一段时间的看板数据
- `python run.py quant-index refresh-breadth [start_date] [end_date]`
  - 专门在股票数据修复后重算涨跌家数相关字段
- `python run.py quant-index repair-recent [trade_day_count]`
  - 重算最近 N 个交易日，默认最近 10 个交易日

当前涨跌家数规则：

- 基于 `stock_daily_data` 当前最终留存行统一计算
- 不再按 `data_source` 把同一天同一只股票拆成多份重复统计

可直接复制的常用命令：

```bash
python run.py quant-index daily
python run.py quant-index daily 2026-04-10
python run.py quant-index backfill 2026-04-01 2026-04-10
python run.py quant-index refresh-breadth 2026-04-01 2026-04-10
python run.py quant-index repair-recent
python run.py quant-index repair-recent 20
```

### 中金所会员持仓排名
用途：

- 抓取中金所会员持仓排名页数据

来源：

- `http://www.cffex.com.cn/ccpm/`

实现方式：

- Playwright 网页抓取，不走 AKShare

写入表：

- `cffex_member_rankings`

支持品种：

- `IF`
- `IH`
- `IC`
- `IM`
- `TS`
- `TF`
- `T`
- `TL`

命令说明：

- `python run.py cffex daily [PRODUCT_CODE ...]`
  - 从库中已存在的最新交易日之后继续追
- `python run.py cffex backfill [PRODUCT_CODE ...]`
  - 从各品种上市日开始回补
- `python run.py cffex single <YYYY-MM-DD> <PRODUCT_CODE>`
  - 手工抓某一天某个品种

可直接复制的常用命令：

```bash
python run.py cffex daily
python run.py cffex daily IF TS
python run.py cffex backfill
python run.py cffex backfill IF IH IC IM
python run.py cffex single 2026-04-10 IF
```

### 汇率与美元指数
用途：

- 维护人民币相关汇率历史
- 单独维护美元指数历史

实际接口：

- `ak.forex_spot_em`
- `ak.forex_hist_em`
- `ak.index_global_hist_em(symbol="美元指数")`

写入表：

- `forex_basic_info`
- `forex_daily_data`

命令说明：

- `python run.py forex backfill [SYMBOL ...]`
  - 先用 `forex_spot_em` 获取品种列表
  - 再逐个调用 `forex_hist_em` 回补历史
- `python run.py forex daily [SYMBOL ...]`
  - 当前普通汇率日更直接用 `forex_hist_em`
  - 不再额外制造“今天的 spot 快照行”
  - 默认只更新以下 7 个符号：
    - `USDCNH`
    - `CNHJPY`
    - `CNHEUR`
    - `CNHHKD`
    - `USDHKD`
    - `USDJPY`
    - `USDEUR`
- `python run.py forex repair-history [SYMBOL ...]`
  - 扫描当天未被历史刷新覆盖到的行并补齐
- `python run.py forex usd-backfill`
  - 用 `index_global_hist_em("美元指数")` 回补美元指数全历史
- `python run.py forex usd-once`
  - 拉取美元指数最新可得历史并刷新最近两行
- `python run.py forex usd-daily`
  - 按固定轮询间隔持续刷新美元指数

说明：

- `runner daily` 中，普通汇率和美元指数是两条独立任务
- 即 `forex_daily` 只跑上面的 7 个汇率，`usd_index_once` 单独跑美元指数

可直接复制的常用命令：

```bash
python run.py forex daily
python run.py forex daily USDCNH USDJPY USDEUR
python run.py forex backfill
python run.py forex repair-history
python run.py forex usd-backfill
python run.py forex usd-once
```

### 场内基金 ETF
用途：

- 维护 ETF 基础信息
- 维护 ETF 当日快照
- 维护 ETF 新浪历史日线

实际接口：

- `ak.fund_etf_category_sina(symbol="ETF基金")`
- `ak.fund_etf_hist_sina(symbol="sh510050")`

写入表：

- `etf_basic_info_sina`
- `etf_daily_data_sina`

命令说明：

- `python run.py etf daily [ETF_CODE ...]`
  - 只调用 `fund_etf_category_sina`
  - 更新 `etf_basic_info_sina`
  - 写入当日 `etf_daily_data_sina`
- `python run.py etf backfill [ETF_CODE ...]`
  - 先用 `fund_etf_category_sina` 获取 ETF 清单
  - 再把所有 `fund_etf_hist_sina` 历史请求先整批提交给 scheduler
  - 最后由程序轮询 scheduler 结果并写库
  - 历史起始日固定为 `2005-02-23`，结束日固定为昨天
- `python run.py etf weekly-repair [ETF_CODE ...]`
  - 周末全量回刷新浪历史
  - 用于修正历史价格变化
- `python run.py etf repair-backfill [ETF_CODE ...]`
  - 扫描两类缺口并循环补到没有待补项为止：
    - `daily_task_failures` 里的 `etf_backfill_history`
    - `etf_basic_info_sina` 中存在、但 `etf_daily_data_sina` 没有任何历史行的 ETF

说明：

- ETF 新链路已经完全切到新浪来源
- 旧表 `etf_basic_info`、`etf_daily_data` 保留但不再更新

可直接复制的常用命令：

```bash
python run.py etf daily
python run.py etf daily 510050 159915
python run.py etf backfill
python run.py etf backfill 510050 159915
python run.py etf weekly-repair
python run.py etf repair-backfill
```

### 中金所期货
用途：

- 维护中金所具体合约日线
- 由具体合约派生股指四类主连 / 月连
- 兼容保留东方财富连续合约历史

实际接口：

- `ak.get_futures_daily`
- `ak.futures_hist_em`

写入表：

- `futures_daily_data`

默认主流程：

- 先用 `get_futures_daily` 抓具体合约
- 再按同日同品种派生：
  - 主连：成交量优先，平手再看持仓量、合约年月、symbol
  - 月连：按完整合约年月选择最近到期，正确处理跨年

派生范围仅包含：

- `IF -> IFM / IFM0`
- `IC -> ICM / ICM0`
- `IH -> IHM / IHM0`
- `IM -> IMM / IMM0`

命令说明：

- `python run.py futures backfill [start_date] [end_date]`
  - 默认主流程
  - 回补 `get_futures_daily` 具体合约和派生主连 / 月连
- `python run.py futures daily [trade_date|start_date end_date]`
  - 默认主流程的日更版本
- `python run.py futures trade-date <YYYY-MM-DD>`
  - 手工抓某个指定交易日
- `python run.py futures market-backfill [start_date] [end_date]`
  - 只跑 `get_futures_daily` 主流程
- `python run.py futures market-daily [trade_date|start_date end_date]`
  - 只跑 `get_futures_daily` 指定日期范围
- `python run.py futures hist-backfill [start_date] [end_date] [HIST_SYMBOL ...]`
  - 单独回补 `futures_hist_em`
- `python run.py futures hist-daily [trade_date|start_date end_date] [HIST_SYMBOL ...]`
  - 单独更新 `futures_hist_em`

`futures_hist_em` 当前支持的连续合约代码：

- `ICM`
- `ICM0`
- `IFM`
- `IFM0`
- `IHM`
- `IHM0`
- `IMM`
- `IMM0`

说明：

- 默认 `backfill` / `daily` 不再依赖 `futures_hist_em`
- 旧 `futures_hist_em` 连续数据仍然保留，并通过 `data_source` 与新派生结果共存
- 历史起始日固定为 `2010-04-16`

可直接复制的常用命令：

```bash
python run.py futures daily
python run.py futures daily 2026-04-10
python run.py futures backfill 2026-04-01 2026-04-10
python run.py futures trade-date 2026-04-10
python run.py futures market-daily 2026-04-10
python run.py futures hist-daily 2026-04-10 IFM IFM0
python run.py futures daily-us-index
python run.py futures daily-us-index-official
python run.py futures daily-hk-index
python run.py futures backfill-us-index 2026-04-01 2026-04-20
python run.py futures backfill-us-index-official 2026-04-01 2026-04-20
python run.py futures backfill-hk-index 2026-04-01 2026-04-20
```

外盘股指期货说明：

- 美股股指期货单独维护 `ES`、`NQ` 的连续/品种级日线，数据源为新浪外盘期货，不走 AK / 东方财富 / 原官方接口。
- 美股 `source_contract_code` 写入 `ES` / `NQ`，`contract_month` 固定写入 `CONTINUOUS`，不再表示具体月份合约。
- 美股 `open_price`、`high_price`、`low_price`、`close_price` 使用新浪日 K；新浪返回为 `0` 的 `volume`、`open_interest`、`settle_price`、`pre_settle_price` 按 `NULL` 写入。
- `daily-us-index-official` / `backfill-us-index-official` 额外维护 CME 官方 `ES` / `NQ` 逐合约结算数据，写入 `futures_us_index_official_*`，不替代现有新浪连续表。
- 港股股指期货单独维护 `HSI`、`HHI`、`HTI` 的具体月份合约，数据源为 HKEX 官方 Daily Market Report。
- 港股股指期货历史回补会先生成港股交易日列表，再按交易日请求 HKEX archive；进度会输出当前请求数、百分比、耗时、ETA、累计写入行数。
- 外盘股指期货写入 `futures_us_index_*` 和 `futures_hk_index_*`，不会混入 `futures_daily_data`。
- 日更可通过统一日常采集服务调用：`POST /collect-us-index-futures-daily`、`POST /collect-us-index-futures-official-daily`、`POST /collect-hk-index-futures-daily`。
- FIT 任务中心对应两个独立采集项：`us_index_futures_daily`（美股交易日）和 `hk_index_futures_daily`（港股交易日）。
- 如果库里已经存在旧版 `futures_us_index_*` 表，先执行 `sql/fix_us_index_futures_sina_source.sql`，把 `contract_month` 放宽到可写入 `CONTINUOUS`。

### 中金所期权日统计网页
用途：

- 抓取中金所 `rtj` 页面上的期权日统计数据

来源：

- `http://www.cffex.com.cn/rtj/`

实现方式：

- Playwright 网页抓取，不走 AKShare

写入表：

- `option_cffex_rtj_daily_data`

覆盖品种：

- 沪深300股指期权：`IO`，上市日 `2019-12-23`
- 中证1000股指期权：`MO`，上市日 `2022-07-22`
- 上证50股指期权：`HO`，上市日 `2022-12-19`

命令说明：

- `python run.py option daily [target_date]`
  - 抓指定日期或当天
- `python run.py option backfill [end_date]`
  - 从最早上市日开始按工作日回补到目标结束日
- `python run.py option repair-backfill [date|start_date end_date]`
  - 扫描指定日期范围内数据库缺失的交易日并补抓

说明：

- 页面成功但无数据，按正常空结果处理
- 页面解析失败、结构变化、超时等才算错误
- 旧 AK 期权表 `option_cffex_spot_data`、`option_cffex_daily_data` 保留但不再更新

可直接复制的常用命令：

```bash
python run.py option daily
python run.py option daily 2026-04-10
python run.py option backfill 2026-04-10
python run.py option repair-backfill 2026-04-01 2026-04-10
```

### 抖音情绪指标
用途：

- 抓取抖音视频并通过 AI 总结生成指数情绪值

来源：

- 抖音网页
- 播放页 AI 总结

写入表：

- `douyin_index_emotion_daily`

命令说明：

- `python run.py douyin backfill`
  - 从账号页抓全量视频并回填
- `python run.py douyin daily`
  - 从最新情绪日期往后追

说明：

- 这是浏览器自动化链路，不依赖 AK scheduler
- 运行期浏览器缓存、截图等产物会写到 `runtime/cache` 和 `runtime/artifacts`

可直接复制的常用命令：

```bash
python run.py douyin daily
python run.py douyin backfill
```

### 表格情绪指标导入
用途：

- 把 Excel 中的情绪数据导入数据库

实现方式：

- `pandas.read_excel`

写入表：

- `excel_index_emotion_daily`

命令说明：

- `python run.py emotion-excel import`
  - 自动从 `data/input/` 或当前目录寻找第一个 `.xlsx`
- `python run.py emotion-excel import 情绪指标.xlsx`
  - 指定文件导入

当前识别的指数列：

- `上证50`
- `沪深300`
- `中证500`
- `中证1000`

可直接复制的常用命令：

```bash
python run.py emotion-excel import
python run.py emotion-excel import 情绪指标.xlsx
```

## 日常采集
统一日更入口：

```bash
python run.py runner daily
```

当前包含的任务：

- `stock_daily`
- `index_daily`
- `cffex_daily`
- `forex_daily`
- `usd_index_once`
- `futures_daily`
- `etf_daily`
- `option_daily`
- `quant_index_daily`

当前不包含：

- `douyin daily`
- `emotion-excel import`
- `etf weekly-repair`
- `stock_temp_service` 的临时后复权采集

失败任务统一重试入口：

```bash
python run.py runner retry-failures
python run.py runner retry-failures stock_daily 20
```

可直接复制的常用命令：

```bash
python run.py runner daily
python run.py runner retry-failures
python run.py runner retry-failures stock_daily 20
python run.py runner retry-failures etf_daily 50
```

当前支持的任务级失败重试名称包括：

- `stock_daily`
- `index_daily`
- `cffex_daily`
- `forex_daily`
- `usd_index_once`
- `futures_daily`
- `etf_daily`
- `option_daily`
- `quant_index_daily`

## 建表脚本与文档
常用 SQL 文件：

- `sql/stock_tables.sql`
- `sql/index_tables.sql`
- `sql/quant_index_tables.sql`
- `sql/cffex_tables.sql`
- `sql/forex_tables.sql`
- `sql/etf_tables.sql`
- `sql/futures_tables.sql`
- `sql/option_rtj_tables.sql`
- `sql/failed_task_tables.sql`
- `sql/ak_scheduler_tables.sql`
- `sql/fix_all_datetime_timezone.sql`

文档：

- `docs/DATABASE_DISPLAY_GUIDE.md`
- `docs/STOCK_TEMP_SERVICE_INTEGRATION.md`

## 备注

- 所有 AK 相关采集任务都默认要求 `ak_scheduler_service` 先启动
- 若数据库里仍有老表使用 `TIMESTAMP`，建议执行一次：
  - `sql/fix_all_datetime_timezone.sql`
- 若怀疑 scheduler 命中了旧进程、空成功缓存或异常复用，优先执行：
  - `python ak_scheduler_service.py health`
  - `python ak_scheduler_service.py doctor`

## 统一日常采集服务

当前 `python stock_temp_service.py serve` 已经可以统一接收日常采集 HTTP 请求。

主要 daily endpoint 包括：

- `POST /collect-stock-daily`
- `POST /collect-index-cn-daily`
- `POST /collect-index-bj50-daily`
- `POST /collect-cffex-daily`
- `POST /collect-forex-daily`
- `POST /collect-usd-index-daily`
- `POST /collect-futures-daily`
- `POST /collect-us-index-futures-daily`
- `POST /collect-us-index-futures-official-daily`
- `POST /collect-hk-index-futures-daily`
- `POST /collect-etf-daily`
- `POST /collect-option-daily`
- `POST /collect-quant-index-daily`
- `POST /collect-index-us-daily`
- `POST /collect-index-hk-daily`
- `POST /collect-index-qvix-daily`
- `POST /collect-index-news-sentiment-daily`
- `POST /collect-index-us-vix-daily`
- `POST /collect-index-us-fear-greed-daily`
- `POST /collect-index-us-hedge-proxy-daily`

统一对接文档：

- `docs/DAILY_COLLECTION_SERVICE_INTEGRATION.md`

专项文档：

- `docs/STOCK_TEMP_SERVICE_INTEGRATION.md`
- `docs/INDEX_TEMP_SERVICE_INTEGRATION.md`

主要依赖链建议由另一个项目按以下顺序调用：

1. `POST /collect-stock-daily`
2. `POST /collect-index-cn-daily`
3. `POST /collect-cffex-daily`
4. `POST /collect-forex-daily`
5. `POST /collect-usd-index-daily`
6. `POST /collect-futures-daily`
7. `POST /collect-etf-daily`
8. `POST /collect-option-daily`
9. `POST /collect-quant-index-daily`

说明：

- `quant_index_daily` 依赖股票、A 股指数、期货等主链数据，应始终放在主链最后。
- 北证 50、US/HK/QVIX、新闻情绪、美股风险情绪等独立链路目前不反向影响 `quant_index_dashboard_daily`，可独立调度。
- 所有 daily endpoint v1 都只接受空 JSON：`{}`。

## 量化指数补充说明

- `python run.py quant-index repair-recent [trade_day_count]`
  - 默认重算 `quant_index_dashboard_daily` 最近 10 个交易日
- `python run.py emotion-excel import [xlsx_path]`
  - 当前按 `emotion_date + index_name` 对 `excel_index_emotion_daily` 执行 upsert
  - 导入完成后，会自动只刷新受影响的 `quant_index_dashboard_daily` 日期
