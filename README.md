# akshareProkect

基于 `AKShare + asyncio + aiomysql + Playwright` 的多源行情采集项目。

当前正式代码位于 [src/akshare_project](/C:/Users/Administrator/PycharmProjects/akshareProkect/src/akshare_project)，正式入口为 [run.py](/C:/Users/Administrator/PycharmProjects/akshareProkect/run.py)。

AKShare 请求统一通过独立调度服务 [ak_scheduler_service.py](/C:/Users/Administrator/PycharmProjects/akshareProkect/ak_scheduler_service.py) 发出；CFFEX 网页抓取和抖音抓取不走该调度服务。

数据库展示说明见 [docs/DATABASE_DISPLAY_GUIDE.md](/C:/Users/Administrator/PycharmProjects/akshareProkect/docs/DATABASE_DISPLAY_GUIDE.md)。

## 启动顺序

1. 执行对应建表 SQL
2. 启动 AK 调度服务
3. 再运行采集命令

调度服务：
```bash
python ak_scheduler_service.py serve
python ak_scheduler_service.py health
```

## 项目结构

```text
akshareProkect/
|-- ak_scheduler_service.py
|-- run.py
|-- config/
|-- sql/
|-- src/akshare_project/
|   |-- collectors/
|   |-- core/
|   |-- db/
|   `-- scheduler/
|-- docs/
|-- data/input/
|-- runtime/
`-- scripts/
```

运行期目录：
- [runtime/logs](/C:/Users/Administrator/PycharmProjects/akshareProkect/runtime/logs)
- [runtime/state](/C:/Users/Administrator/PycharmProjects/akshareProkect/runtime/state)
- [runtime/cache](/C:/Users/Administrator/PycharmProjects/akshareProkect/runtime/cache)
- [runtime/artifacts](/C:/Users/Administrator/PycharmProjects/akshareProkect/runtime/artifacts)

## 数据链路与接口

### 股票
- 用途：A 股历史行情、日更、缺失日期回补
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
- 来源：[中金所会员排名页面](http://www.cffex.com.cn/ccpm/)
- 方式：Playwright 网页抓取，不走 AKShare
- 写入表：
  - `cffex_member_rankings`
- 命令：
```bash
python run.py cffex backfill
python run.py cffex daily
```

### 抖音情绪指标
- 来源：抖音网页播放页 + AI 总结
- 不走 AKShare
- 写入表：
  - `douyin_index_emotion_daily`
- 命令：
```bash
python run.py douyin backfill
python run.py douyin daily
```

### 外汇与美元指数
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

普通汇率日更当前默认只更新：
- `USDCNH`
- `CNHJPY`
- `CNHEUR`
- `CNHHKD`
- `USDHKD`
- `USDJPY`
- `USDEUR`

### ETF
- 接口：
  - `ak.fund_etf_spot_em`
  - `ak.fund_etf_spot_ths`
  - `ak.fund_etf_hist_em`
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

### 中金所期货
- 接口：
  - `ak.get_futures_daily`
  - `ak.futures_hist_em`（兼容保留，不再作为默认主流程）
- 写入表：
  - `futures_daily_data`
- 默认规则：
  - 先写入 `get_futures_daily` 返回的具体合约日线
  - 再从 `IF / IC / IH / IM` 具体合约中派生主连和月连
  - 主连来源：`data_source='get_futures_daily_derived'`
  - 月连规则：最近到期合约，按完整 `YYMM` 处理跨年
- 命令：
```bash
python run.py futures backfill
python run.py futures daily
python run.py futures trade-date 2026-03-25
python run.py futures market-backfill
python run.py futures market-daily
python run.py futures hist-backfill
python run.py futures hist-daily
```

说明：
- `backfill` / `daily` 默认只跑 `get_futures_daily` 具体合约和派生连续数据
- `trade-date` 用来显式抓取某一个指定交易日的数据
- `hist-backfill` / `hist-daily` 仅保留为兼容入口，不再属于默认主流程

连续合约派生符号：
- `IF -> IFM / IFM0`
- `IC -> ICM / ICM0`
- `IH -> IHM / IHM0`
- `IM -> IMM / IMM0`

如果数据库里已经存在 `futures_daily_data`，上线前请先执行：
- [sql/fix_futures_unique_key.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/fix_futures_unique_key.sql)

### 中金所期权
- 当前主链路已切换为中金所日统计页面，不再使用 AKShare 的 `option_cffex_*_sina`
- 来源：[http://www.cffex.com.cn/rtj/](http://www.cffex.com.cn/rtj/)
- 方式：Playwright 网页抓取，查询步骤为：
  - 选择 `期权`
  - 下拉选择 `全部`
  - 选择对应日期
  - 点击查询
  - 解析当日全部期权行
- 写入表：
  - 新表 `option_cffex_rtj_daily_data`
  - 旧表 `option_cffex_spot_data`、`option_cffex_daily_data` 保留旧数据，但不再更新
- 命令：
```bash
python run.py option backfill
python run.py option daily
```

上市日期与过滤规则：
- 沪深300股指期权：`2019-12-23`
- 中证1000股指期权：`2022-07-22`
- 上证50股指期权：`2022-12-19`
- 合约前缀映射：
  - `IO` -> `HS300`
  - `MO` -> `ZZ1000`
  - `HO` -> `SZ50`

期权历史回补默认从 `2019-12-23` 开始，到昨日结束；日更默认查询当天。

### Excel 情绪指标
- 来源：`pandas.read_excel`
- 写入表：
  - `excel_index_emotion_daily`
- 命令：
```bash
python run.py emotion-excel import
python run.py emotion-excel import data/input/情绪指标.xlsx
```

## 一键日更

命令：
```bash
python run.py runner daily
```

当前包含：
- `index_daily`
- `cffex_daily`
- `forex_daily`
- `usd_index_once`
- `futures_daily`
- `etf_daily`
- `option_daily`

当前不包含：
- 股票日更
- 抖音情绪
- Excel 情绪导入
- ETF 周末前复权回刷

## 失败任务

失败任务表：
- [sql/failed_task_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/failed_task_tables.sql)

统一重试入口：
```bash
python run.py runner retry-failures
python run.py runner retry-failures option_daily
```

## 建表 SQL

主要 SQL 文件：
- [sql/stock_spot_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/stock_spot_tables.sql)
- [sql/index_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/index_tables.sql)
- [sql/cffex_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/cffex_tables.sql)
- [sql/forex_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/forex_tables.sql)
- [sql/futures_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/futures_tables.sql)
- [sql/fix_futures_unique_key.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/fix_futures_unique_key.sql)
- [sql/etf_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/etf_tables.sql)
- [sql/option_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/option_tables.sql)
- [sql/option_rtj_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/option_rtj_tables.sql)
- [sql/douyin_emotion_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/douyin_emotion_tables.sql)
- [sql/excel_emotion_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/excel_emotion_tables.sql)
- [sql/failed_task_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/failed_task_tables.sql)
- [sql/ak_scheduler_tables.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/ak_scheduler_tables.sql)
- [sql/fix_all_datetime_timezone.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/fix_all_datetime_timezone.sql)

## 数据库时间说明

项目当前统一按以下规则处理时间字段：
- 数据库连接 session 固定 `+08:00`
- 元数据时间字段统一使用 `DATETIME`
- 不再使用 `TIMESTAMP` 作为业务表的 `created_at / updated_at`

如果你的现有库历史上已经有 8 小时偏移问题，请执行：
- [sql/fix_all_datetime_timezone.sql](/C:/Users/Administrator/PycharmProjects/akshareProkect/sql/fix_all_datetime_timezone.sql)

## 数据库配置

数据库配置文件：
- [config/db_info.json](/C:/Users/Administrator/PycharmProjects/akshareProkect/config/db_info.json)

当前读取字段：
- `host`
- `port`
- `user`
- `passwd`
- `database`
- `charset`
- `timezone`

示例：
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
