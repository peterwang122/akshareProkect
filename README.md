# AkShare Project

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
2. 如需接收另一个项目的股票前复权临时采集请求，再启动：`python stock_temp_service.py serve`
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

### 统一采集入口
```bash
python run.py stock daily [stock_code ...]
python run.py stock backfill [stock_code ...]
python run.py stock repair-backfill [stock_code ...]

python run.py index daily
python run.py index backfill

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

## AK 调度服务
AKShare 请求统一走独立调度服务，不允许业务采集代码直接绕过调度器去请求 AK。

启动与自检：

```bash
python ak_scheduler_service.py serve
python ak_scheduler_service.py health
python ak_scheduler_service.py doctor
```

当前调度规则：

- 东方财富、新浪、同花顺三条独立队列
- 三条队列彼此独立，可以并发
- 每条队列默认单独节流
- 默认节流、重试、熔断开关等由 `config/ak_scheduler.json` 控制
- `/health` 会返回当前调度服务版本、PID、启动时间等信息
- `doctor` 用于排查 scheduler 是否还在产生空成功、旧进程未退出等问题

当前已经接入调度服务的主要 AK 函数来源：

- 东方财富：`stock_zh_a_spot`、`stock_zh_index_daily_em`、`forex_hist_em`、`index_global_hist_em`、`get_futures_daily` 等
- 新浪：`stock_zh_a_hist_tx`、`stock_zh_index_spot_sina`、`index_zh_a_hist`、`fund_etf_category_sina`、`fund_etf_hist_sina` 等
- 同花顺：当前主要保留注册能力，具体是否使用以 collector 实现为准

## 模块说明

### 股票
用途：

- 维护统一股票信息表
- 维护非复权日线 / 日快照
- 提供供外部项目调用的前复权临时采集服务

实际接口：

- `ak.stock_info_sh_name_code`
- `ak.stock_info_sz_name_code`
- `ak.stock_info_bj_name_code`
- `ak.stock_zh_a_spot`
- `ak.stock_zh_a_hist_tx`
- `ak.stock_zh_a_daily(adjust="qfq")`

写入表：

- `stock_info_all`
- `stock_daily_data`
- `stock_qfq_daily_data`

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

### 股票前复权临时采集服务
用途：

- 提供给另一个项目按股票代码触发前复权数据刷新

服务命令：

```bash
python stock_temp_service.py serve
python stock_temp_service.py health
```

HTTP 接口：

- `GET /health`
- `POST /collect`

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
- 调用 `stock_zh_a_daily(adjust="qfq")`
- 若请求区间与最近一次该股票区间完全一致，则返回 `UNCHANGED`
- 若区间发生变化，则先删除该股票在 `stock_qfq_daily_data` 的全部旧数据，再整批重写
- 前复权结果会在写库前按前一交易日收盘价自行计算：
  - `price_change_amount`
  - `price_change_rate`

对接文档：

- `docs/STOCK_TEMP_SERVICE_INTEGRATION.md`

### 指数
用途：

- 维护指数基础信息
- 维护指数日线和日快照

实际接口：

- `ak.stock_zh_index_spot_sina`
- `ak.index_zh_a_hist`
- `ak.stock_zh_index_daily_em`

写入表：

- `index_basic_info`
- `index_daily_data`

命令说明：

- `python run.py index daily`
  - 从 `stock_zh_index_spot_sina` 抓取快照
  - 更新 `index_basic_info`
  - 更新 `index_daily_data`
- `python run.py index backfill`
  - 先抓指数快照拿到指数清单
  - 历史优先走 `index_zh_a_hist`
  - 若失败或无数据，再回退 `stock_zh_index_daily_em`

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

当前涨跌家数规则：

- 基于 `stock_daily_data` 当前最终留存行统一计算
- 不再按 `data_source` 把同一天同一只股票拆成多份重复统计

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

### ETF
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

### Excel 情绪指标导入
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
- `stock_temp_service` 的临时前复权采集

失败任务统一重试入口：

```bash
python run.py runner retry-failures
python run.py runner retry-failures stock_daily 20
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

## SQL 与文档
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

## Quant Index Notes

- `python run.py quant-index repair-recent [trade_day_count]`
  - default recalculates the latest 10 trade dates in `quant_index_dashboard_daily`
- `python run.py emotion-excel import [xlsx_path]`
  - now upserts `excel_index_emotion_daily` by `emotion_date + index_name`
  - after import, it automatically refreshes `quant_index_dashboard_daily` only for the affected trade dates
