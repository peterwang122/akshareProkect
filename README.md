# akshareProkect

基于 `AKShare + asyncio + aiomysql + Playwright` 的多源行情采集项目。当前采集代码统一放在 [`src/akshare_project`](/C:/Users/Administrator/PycharmProjects/akshareProkect/src/akshare_project) 下，正式业务命令仍通过 [`run.py`](/C:/Users/Administrator/PycharmProjects/akshareProkect/run.py) 分发；但 **所有 AKShare 请求现在都必须先经过独立调度服务**，服务入口不走 `run.py`。

数据库展示说明见 [docs/DATABASE_DISPLAY_GUIDE.md](/C:/Users/Administrator/PycharmProjects/akshareProkect/docs/DATABASE_DISPLAY_GUIDE.md)。

## 1. 先启动 AK 调度服务

AK 调度服务负责统一承接本项目的所有 AKShare 请求，按来源分组做独立节流、重试和持久化排队。

启动：

```bash
python ak_scheduler_service.py serve
```

健康检查：

```bash
python ak_scheduler_service.py health
```

调度服务特性：

- 东方财富 / 新浪 / 同花顺三条独立队列
- 每个来源各自 `2` 秒最多发出 `1` 个请求
- 三条队列之间可并发
- 指数退避从 `120` 秒开始
- 熔断规则通过配置控制，默认关闭
- 每个 AK 请求按单次调用粒度持久化到数据库表 `ak_request_jobs`
- 支持 `parent_job_id / root_job_id / workflow_name` 父子依赖

配置文件：

- [config/ak_scheduler.json](/C:/Users/Administrator/PycharmProjects/akshareProkect/config/ak_scheduler.json)

建表 SQL：

- [sql/ak_scheduler_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/ak_scheduler_tables.sql)

## 2. 项目结构

```text
akshareProkect/
|-- ak_scheduler_service.py
|-- run.py
|-- README.md
|-- requirements.txt
|-- config/
|   |-- db_info.json
|   `-- ak_scheduler.json
|-- sql/
|-- src/
|   `-- akshare_project/
|       |-- collectors/
|       |-- core/
|       |-- db/
|       `-- scheduler/
|-- docs/
|-- data/input/
|-- runtime/
|   |-- logs/
|   |-- state/
|   |-- cache/
|   `-- artifacts/
`-- scripts/
```

说明：

- [`src/akshare_project/collectors`](/C:/Users/Administrator/PycharmProjects/akshareProkect/src/akshare_project/collectors)：业务采集逻辑
- [`src/akshare_project/core`](/C:/Users/Administrator/PycharmProjects/akshareProkect/src/akshare_project/core)：日志、路径、进度、重试、调度客户端
- [`src/akshare_project/db`](/C:/Users/Administrator/PycharmProjects/akshareProkect/src/akshare_project/db)：数据库工具
- [`src/akshare_project/scheduler`](/C:/Users/Administrator/PycharmProjects/akshareProkect/src/akshare_project/scheduler)：独立 AK 调度服务实现
- [`runtime/logs`](/C:/Users/Administrator/PycharmProjects/akshareProkect/runtime/logs)：模块日志
- [`runtime/state`](/C:/Users/Administrator/PycharmProjects/akshareProkect/runtime/state)：断点与进度
- [`runtime/cache`](/C:/Users/Administrator/PycharmProjects/akshareProkect/runtime/cache)：浏览器缓存等
- [`runtime/artifacts`](/C:/Users/Administrator/PycharmProjects/akshareProkect/runtime/artifacts)：截图、帧图等运行产物

## 3. 安装与配置

环境要求：

- Python 3.9+
- MySQL 5.7 / 8.0
- 可访问 AKShare 对应数据源
- 抖音/CFFEX 相关网页抓取需要可用浏览器环境

安装依赖：

```bash
pip install -r requirements.txt
playwright install chromium
```

数据库配置文件：

- [config/db_info.json](/C:/Users/Administrator/PycharmProjects/akshareProkect/config/db_info.json)

当前代码读取的密码字段是 `passwd`：

```json
{
  "host": "127.0.0.1",
  "port": 3306,
  "user": "root",
  "passwd": "your_password",
  "database": "your_database",
  "charset": "utf8mb4",
  "timezone": "+08:00"
}
```

说明：

- 当前代码即使不写 `timezone`，也会默认按 `+08:00` 建立数据库会话
- 如果你后面要切换到别的时区，也可以在 `db_info.json` 里显式配置 `timezone`

输入文件目录：

- [data/input](/C:/Users/Administrator/PycharmProjects/akshareProkect/data/input)

常见输入文件：

- `allstock_em.csv`
- `allstock.csv`
- `情绪指标.xlsx`

## 4. 数据链路与接口

### 股票

- 用途：A 股历史行情回补、指定日期缺失修复、股票基础信息与估值补充
- 接口：
  - `ak.stock_individual_info_em`
  - `ak.stock_zh_a_hist`
  - `ak.stock_zh_a_spot_em`
- 写入表：
  - `stock_basic_info`
  - `stock_data`
- 命令：

```bash
python run.py stock backfill
python run.py stock daily
python run.py stock repair-missing-date 2026-03-16
```

### 指数

- 用途：指数基础信息、历史日线、当日快照
- 接口：
  - `ak.stock_zh_index_spot_sina`
  - `ak.index_zh_a_hist`
  - `ak.stock_zh_index_daily_em`
- 写入表：
  - `index_basic_info`
  - `index_daily_data`
- 命令：

```bash
python run.py index backfill
python run.py index daily
```

### CFFEX 会员排名

- 用途：IF / IH / IC / IM / TS / TF / T / TL 会员排名
- 来源：
  - [中金所会员排名页面](http://www.cffex.com.cn/ccpm/)
  - 通过 Playwright 抓取，不经过 AKShare
- 写入表：
  - `cffex_member_rankings`
- 命令：

```bash
python run.py cffex backfill
python run.py cffex daily
python run.py cffex single 2025-03-14 IM
```

### 抖音情绪指标

- 用途：从抖音号 `1368194981` 的视频中提取上证50、沪深300、中证500、中证1000情绪指标
- 来源：
  - 抖音网页播放页
  - “识别画面” + AI 总结
- 写入表：
  - `douyin_index_emotion_daily`
- 命令：

```bash
python run.py douyin backfill
python run.py douyin daily
```

### 外汇与美元指数

- 用途：汇率基础信息、历史日线、当日快照、美元指数
- 接口：
  - `ak.forex_spot_em`
  - `ak.forex_hist_em`
  - `ak.index_global_hist_em(symbol="美元指数")`
- 写入表：
  - `forex_basic_info`
  - `forex_daily_data`
- 命令：

```bash
python run.py forex backfill
python run.py forex daily
python run.py forex repair-history
python run.py forex usd-backfill
python run.py forex usd-daily
python run.py forex usd-once
```

当前 `forex daily` 默认只更新：

- `USDCNH`
- `CNHJPY`
- `CNHEUR`
- `CNHHKD`
- `USDHKD`
- `USDJPY`
- `USDEUR`

美元指数走独立命令，不混在普通汇率 spot 日更里。

### ETF

- 用途：ETF 基础信息、前复权历史、当日快照、周末历史回刷、历史失败补扫
- 接口：
  - `ak.fund_etf_spot_em`
  - `ak.fund_etf_spot_ths`
  - `ak.fund_etf_hist_em`
- 固定参数：
  - 历史起始日：`2005-02-23`
  - 历史结束日：昨日
  - `period="daily"`
  - `adjust="qfq"`
- 写入表：
  - `etf_basic_info`
  - `etf_daily_data`
- 命令：

```bash
python run.py etf backfill
python run.py etf daily
python run.py etf weekly-repair
python run.py etf repair-backfill
```

说明：

- ETF spot 默认优先 `fund_etf_spot_em`，失败时回退 `fund_etf_spot_ths`
- `weekly-repair` 会覆盖昨日及更早历史行，修正分红导致的前复权整体变化
- `repair-backfill` 会循环扫描 `daily_task_failures` 中的 `etf_backfill_history` 和“空历史 ETF”

### 中金所期货

- 用途：中金所期货日线与主连/当月连续日线
- 接口：
  - `ak.get_futures_daily`
  - `ak.futures_hist_em`
- 写入表：
  - `futures_daily_data`
- 命令：

```bash
python run.py futures backfill
python run.py futures daily
python run.py futures market-backfill
python run.py futures market-daily
python run.py futures hist-backfill
python run.py futures hist-daily
```

### 中金所指数期权

- 用途：上证50、沪深300、中证1000指数期权的合约链和日线
- 接口：
  - `ak.option_cffex_sz50_list_sina`
  - `ak.option_cffex_sz50_spot_sina`
  - `ak.option_cffex_sz50_daily_sina`
  - `ak.option_cffex_hs300_list_sina`
  - `ak.option_cffex_hs300_spot_sina`
  - `ak.option_cffex_hs300_daily_sina`
  - `ak.option_cffex_zz1000_list_sina`
  - `ak.option_cffex_zz1000_spot_sina`
  - `ak.option_cffex_zz1000_daily_sina`
- 写入表：
  - `option_cffex_spot_data`
  - `option_cffex_daily_data`
- 命令：

```bash
python run.py option backfill
python run.py option daily
python run.py option repair-missing-date 2026-03-16
```

### Excel 情绪导入

- 用途：将整理好的 Excel 情绪指标导入数据库
- 来源：
  - `pandas.read_excel`
- 写入表：
  - `excel_index_emotion_daily`
- 命令：

```bash
python run.py emotion-excel import
python run.py emotion-excel import data/input/情绪指标.xlsx
```

## 5. 一键日更与失败任务

一键日更：

```bash
python run.py runner daily
```

失败任务重试：

```bash
python run.py runner retry-failures
python run.py runner retry-failures option_daily 20
```

`runner daily` 当前包含：

- `index_daily`
- `cffex_daily`
- `forex_daily`
- `usd_index_once`
- `futures_daily`
- `etf_daily`
- `option_daily`

当前不包含：

- 股票日更
- ETF 周末回刷
- 抖音情绪
- Excel 情绪导入

## 6. SQL 文件

- [sql/stock_spot_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/stock_spot_tables.sql)
- [sql/index_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/index_tables.sql)
- [sql/cffex_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/cffex_tables.sql)
- [sql/douyin_emotion_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/douyin_emotion_tables.sql)
- [sql/forex_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/forex_tables.sql)
- [sql/etf_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/etf_tables.sql)
- [sql/futures_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/futures_tables.sql)
- [sql/option_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/option_tables.sql)
- [sql/excel_emotion_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/excel_emotion_tables.sql)
- [sql/failed_task_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/failed_task_tables.sql)
- [sql/ak_scheduler_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/ak_scheduler_tables.sql)

## 7. 日志、状态与运行产物

- 日志目录：[runtime/logs](/C:/Users/Administrator/PycharmProjects/akshareProkect/runtime/logs)
- 状态目录：[runtime/state](/C:/Users/Administrator/PycharmProjects/akshareProkect/runtime/state)
- 缓存目录：[runtime/cache](/C:/Users/Administrator/PycharmProjects/akshareProkect/runtime/cache)
- 产物目录：[runtime/artifacts](/C:/Users/Administrator/PycharmProjects/akshareProkect/runtime/artifacts)

新增调度服务日志：

- `runtime/logs/ak_scheduler.log`

## 8. 使用顺序建议

AKShare 相关任务建议按这个顺序运行：

1. 先执行 SQL 建表
2. 启动调度服务：`python ak_scheduler_service.py serve`
3. 另开终端执行采集命令，例如：

```bash
python run.py etf daily
python run.py forex daily
python run.py runner daily
```

注意：

- CFFEX 和抖音链路不走 AK 调度服务
- 其余股票、指数、外汇、ETF、期货、期权链路都会通过调度服务发起 AK 请求
