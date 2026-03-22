# akshareProkect

基于 `AKShare + asyncio + aiomysql + Playwright` 的数据采集入库项目。当前统一使用新入口：

```bash
python run.py <domain> <command> [args]
```

正式入口已经切换到 [run.py](/C:/Users/Administrator/PycharmProjects/akshareProkect/run.py)。旧脚本已移到 [scripts/legacy](/C:/Users/Administrator/PycharmProjects/akshareProkect/scripts/legacy)，不再作为正式命令入口。

数据库展示说明见 [docs/DATABASE_DISPLAY_GUIDE.md](/C:/Users/Administrator/PycharmProjects/akshareProkect/docs/DATABASE_DISPLAY_GUIDE.md)。

## 目录结构

```text
akshareProkect/
|-- run.py
|-- README.md
|-- requirements.txt
|-- config/
|   |-- db_info.json
|   `-- get_config_path.py
|-- sql/
|   |-- stock_spot_tables.sql
|   |-- index_tables.sql
|   |-- cffex_tables.sql
|   |-- douyin_emotion_tables.sql
|   |-- forex_tables.sql
|   |-- futures_tables.sql
|   |-- option_tables.sql
|   |-- excel_emotion_tables.sql
|   `-- failed_task_tables.sql
|-- src/
|   `-- akshare_project/
|       |-- collectors/
|       |-- core/
|       `-- db/
|-- docs/
|   `-- DATABASE_DISPLAY_GUIDE.md
|-- data/
|   `-- input/
|-- runtime/
|   |-- logs/
|   |-- state/
|   |-- cache/
|   `-- artifacts/
`-- scripts/
    |-- legacy/
    `-- test.py / test1.py
```

## 安装与配置

### 环境要求

- Python 3.9+
- MySQL 5.7 或 8.0
- 可访问 AKShare 数据源和中金所网页

### 安装依赖

```bash
pip install -r requirements.txt
playwright install chromium
```

### 数据库配置

数据库配置文件路径：

- [config/db_info.json](/C:/Users/Administrator/PycharmProjects/akshareProkect/config/db_info.json)

当前代码读取的密码字段名是 `passwd`：

```json
{
  "host": "127.0.0.1",
  "port": 3306,
  "user": "root",
  "passwd": "your_password",
  "database": "your_database",
  "charset": "utf8mb4"
}
```

### 输入文件目录

建议将输入文件放在 [data/input](/C:/Users/Administrator/PycharmProjects/akshareProkect/data/input)：

- `allstock_em.csv`
- `allstock.csv`
- `情绪指标.xlsx`

其中：

- 股票链路会优先读取 `data/input/allstock_em.csv`
- Excel 情绪导入会优先扫描 `data/input/*.xlsx`

## 数据链路与接口说明

### 1. 股票

- 用途：A 股历史行情回补、按指定日期修复缺失日线、同步股票基础信息和估值字段
- 主要接口：
  - `ak.stock_individual_info_em`
  - `ak.stock_zh_a_hist`
  - `ak.stock_zh_a_spot_em`
- 写入表：
  - `stock_basic_info`
  - `stock_data`
- 备注：
  - 正式日常采集仍走 `stock_zh_a_hist`
  - `stock_zh_a_spot_em` 仅用于基础信息与估值字段补充，不直接作为 `stock_data` 正式日线来源

### 2. 指数

- 用途：指数基础信息、历史日线、当日快照
- 主要接口：
  - `ak.stock_zh_index_spot_sina`
  - `ak.index_zh_a_hist`
  - `ak.stock_zh_index_daily_em`
- 写入表：
  - `index_basic_info`
  - `index_daily_data`
- 备注：
  - 日更优先使用 `stock_zh_index_spot_sina`
  - 历史回补会在 `index_zh_a_hist` 与 `stock_zh_index_daily_em` 之间做兜底

### 3. CFFEX 会员排名

- 用途：中金所 IF/IH/IC/IM/TS/TF/T/TL 的成交、持买、持卖排名
- 数据来源：
  - [中金所会员排名页面](http://www.cffex.com.cn/ccpm/)
  - 通过 `Playwright + HTML 解析` 抓取，不是 AKShare 接口
- 写入表：
  - `cffex_member_rankings`
- 备注：
  - 已过滤 `合计` 行
  - 支持历史回补、当日同步、单日调试

### 4. 抖音情绪指标

- 用途：从抖音号 `1368194981` 的视频中提取上证50、沪深300、中证500、中证1000情绪指标
- 数据来源：
  - 抖音网页播放页
  - 页面内 `识别画面` + AI 总结面板
  - 必要时 OCR 兜底
- 写入表：
  - `douyin_index_emotion_daily`
- 备注：
  - 不是 AKShare 数据源
  - 浏览器缓存会写入 `runtime/cache/douyin_playwright_profile`

### 5. 外汇

- 用途：外汇品种基础信息、历史日线、当日快照、美元指数
- 主要接口：
  - `ak.forex_spot_em`
  - `ak.forex_hist_em`
  - `ak.index_global_hist_em(symbol="美元指数")`
- 写入表：
  - `forex_basic_info`
  - `forex_daily_data`
- 备注：
  - 日更会更新今天快照，并尝试刷新最近已收盘历史日
  - 可手动执行 `repair-history`，把仍停留在 `forex_spot_em` 口径的历史记录回刷成 `forex_hist_em`

### 6. 中金所期货

- 用途：中金所期货日线行情
- 主要接口：
  - `ak.get_futures_daily`
  - `ak.futures_hist_em`
- 默认行为：
  - `backfill` / `daily` 会同时执行这两条链路
- `futures_hist_em` 固定采集标的：
  - `ICM`：中证500股指主连
  - `ICM0`：中证500股指当月连续
  - `IFM`：沪深300股指主连
  - `IFM0`：沪深300股指当月连续
  - `IHM`：上证50股指主连
  - `IHM0`：上证50股指当月连续
  - `IMM`：中证1000股指主连
  - `IMM0`：中证1000股指当月连续
- 固定参数：
  - `market="CFFEX"`：用于 `ak.get_futures_daily`
  - `period="daily"`：用于 `ak.futures_hist_em`
  - 回补起始日：`2010-04-16`
- 写入表：
  - `futures_daily_data`
- 备注：
  - 两种来源都会写入 `futures_daily_data`，通过 `data_source` 区分
  - `ak.get_futures_daily` 主要用于原来的中金所期货日线链路
  - `ak.futures_hist_em` 主要用于主连和当月连续链路
  - 通过 `start_date` 和 `end_date` 控制历史区间与日更区间
  - 接口缺失字段落库时写为 `NULL`，作为 `NA` 处理

### 7. 中金所指数期权

- 用途：上证50、沪深300、中证1000指数期权的合约链与日线行情
- 主要接口：
  - 上证50：
    - `ak.option_cffex_sz50_list_sina`
    - `ak.option_cffex_sz50_spot_sina`
    - `ak.option_cffex_sz50_daily_sina`
  - 沪深300：
    - `ak.option_cffex_hs300_list_sina`
    - `ak.option_cffex_hs300_spot_sina`
    - `ak.option_cffex_hs300_daily_sina`
  - 中证1000：
    - `ak.option_cffex_zz1000_list_sina`
    - `ak.option_cffex_zz1000_spot_sina`
    - `ak.option_cffex_zz1000_daily_sina`
- 写入表：
  - `option_cffex_spot_data`
  - `option_cffex_daily_data`
- 备注：
  - `spot` 表存合约链快照
  - `daily` 表存最终日线
  - 请求成功但全天无成交、无日线数据的期权按正常情况跳过，不记失败

### 8. Excel 情绪导入

- 用途：将整理好的 Excel 情绪指标批量导入数据库
- 数据来源：
  - `pandas.read_excel`
- 写入表：
  - `excel_index_emotion_daily`

## 数据库表概览

| 模块 | 主要表 |
|---|---|
| 股票 | `stock_basic_info`, `stock_data` |
| 指数 | `index_basic_info`, `index_daily_data` |
| CFFEX 排名 | `cffex_member_rankings` |
| 抖音情绪 | `douyin_index_emotion_daily` |
| 外汇 | `forex_basic_info`, `forex_daily_data` |
| 期货 | `futures_daily_data` |
| 期权 | `option_cffex_spot_data`, `option_cffex_daily_data` |
| Excel 情绪 | `excel_index_emotion_daily` |
| 失败任务 | `daily_task_failures` |

## 运行命令

### 股票

```bash
python run.py stock backfill
python run.py stock daily
python run.py stock repair-missing-date 2026-03-16
```

### 指数

```bash
python run.py index backfill
python run.py index daily
```

### CFFEX 会员排名

```bash
python run.py cffex backfill
python run.py cffex backfill IF IH IM
python run.py cffex daily
python run.py cffex daily T TF TL
python run.py cffex single 2025-03-14 IM
```

### 抖音情绪

```bash
python run.py douyin backfill
python run.py douyin daily
```

### 外汇与美元指数

```bash
python run.py forex backfill
python run.py forex backfill USDCNH EURUSD GBPUSD
python run.py forex daily
python run.py forex daily USDCNH EURUSD GBPUSD
python run.py forex repair-history
python run.py forex repair-history USDCNH EURUSD GBPUSD
python run.py forex usd-backfill
python run.py forex usd-daily
python run.py forex usd-once
```

### 中金所期货

```bash
python run.py futures backfill
python run.py futures daily
python run.py futures backfill 2010-04-16 2026-03-22 IFM IFM0
python run.py futures daily 2026-03-22 ICM IMM0
python run.py futures market-backfill
python run.py futures market-daily
python run.py futures hist-backfill
python run.py futures hist-daily 2026-03-22 IFM IFM0
```

### 中金所期权

```bash
python run.py option backfill
python run.py option daily
python run.py option repair-missing-date 2026-03-16
```

### Excel 情绪导入

```bash
python run.py emotion-excel import
python run.py emotion-excel import data/input/情绪指标.xlsx
```

### 一键日更与失败任务

```bash
python run.py runner daily
python run.py runner retry-failures
python run.py runner retry-failures option_daily 20
```

`runner daily` 当前包含：

- `index_daily`
- `cffex_daily`
- `forex_daily`
- `usd_index_once`
- `futures_daily`
- `option_daily`

`runner daily` 当前不包含：

- 股票日更
- 抖音情绪采集
- Excel 情绪导入

## 日志、状态与运行产物

### 日志目录

- [runtime/logs](/C:/Users/Administrator/PycharmProjects/akshareProkect/runtime/logs)

按模块输出单一日志文件，例如：

- `stock.log`
- `index.log`
- `cffex.log`
- `forex.log`
- `futures.log`
- `option.log`
- `douyin_emotion.log`
- `runner.log`
- `failed_tasks.log`
- `excel_emotion.log`

### 状态目录

- [runtime/state](/C:/Users/Administrator/PycharmProjects/akshareProkect/runtime/state)

用于保存断点与进度，例如：

- `stock.progress`
- `index.progress`
- `cffex.progress`
- `forex.progress`
- `option.progress`
- `douyin_emotion.progress`
- `futures.progress`

### 运行产物目录

- 缓存：[runtime/cache](/C:/Users/Administrator/PycharmProjects/akshareProkect/runtime/cache)
- 产物：[runtime/artifacts](/C:/Users/Administrator/PycharmProjects/akshareProkect/runtime/artifacts)

当前典型内容：

- 抖音浏览器 profile：`runtime/cache/douyin_playwright_profile`
- 抖音截图/帧图：`runtime/artifacts/douyin_emotion_frames`

## SQL 文件

- [sql/stock_spot_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/stock_spot_tables.sql)
- [sql/index_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/index_tables.sql)
- [sql/cffex_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/cffex_tables.sql)
- [sql/douyin_emotion_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/douyin_emotion_tables.sql)
- [sql/forex_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/forex_tables.sql)
- [sql/futures_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/futures_tables.sql)
- [sql/option_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/option_tables.sql)
- [sql/excel_emotion_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/excel_emotion_tables.sql)
- [sql/failed_task_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/failed_task_tables.sql)

## 说明

- 正式入口已经切换到 [run.py](/C:/Users/Administrator/PycharmProjects/akshareProkect/run.py)。
- 旧入口脚本已移到 [scripts/legacy](/C:/Users/Administrator/PycharmProjects/akshareProkect/scripts/legacy)，仅作历史保留。
- 文件日志只负责运行诊断；失败任务补采仍以 `daily_task_failures` 为数据库级追踪来源。
