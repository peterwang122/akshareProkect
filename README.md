# AkShare Project

## 项目概览
本项目用于采集并维护多类金融市场数据，当前覆盖：

- 股票
- 指数
- 中金所会员排名
- 外汇与美元指数
- ETF
- 中金所期货
- 中金所期权网页日统计
- 量化指数看板预计算
- 抖音情绪指标
- Excel 情绪指标导入

项目已经统一为包结构，正式入口分为两类：

- 采集命令入口：`python run.py ...`
- 独立服务入口：
  - `python ak_scheduler_service.py serve`
  - `python stock_temp_service.py serve`

## 运行前准备
### 1. 安装依赖
```bash
pip install -r requirements.txt
playwright install chromium
```

### 2. 配置数据库
数据库配置文件：

- [config/db_info.json](C:\Users\Administrator\PycharmProjects\akshareProkect\config\db_info.json)

字段说明：

- `host`
- `port`
- `user`
- `passwd`
- `database`
- `charset`
- `timezone`

推荐：

```json
"timezone": "+08:00"
```

项目所有新表的时间元数据都使用 `DATETIME`，并且数据库连接会在 session 内固定执行：

```sql
SET time_zone = '+08:00'
```

### 3. 初始化运行目录
项目运行时文件统一落在：

- [runtime/logs](C:\Users\Administrator\PycharmProjects\akshareProkect\runtime\logs)
- [runtime/state](C:\Users\Administrator\PycharmProjects\akshareProkect\runtime\state)
- [runtime/cache](C:\Users\Administrator\PycharmProjects\akshareProkect\runtime\cache)
- [runtime/artifacts](C:\Users\Administrator\PycharmProjects\akshareProkect\runtime\artifacts)

## AK 调度服务
所有 AKShare 请求统一通过独立调度服务发出，不允许业务模块直接绕过。

启动：

```bash
python ak_scheduler_service.py serve
```

健康检查：

```bash
python ak_scheduler_service.py health
```

当前调度规则：

- 东方财富、新浪、同花顺三条独立队列
- 每条队列默认 2 秒最多发出 1 个请求
- 三条队列之间可以并发
- 失败重试从 2 秒回退开始，具体策略见 [config/ak_scheduler.json](C:\Users\Administrator\PycharmProjects\akshareProkect\config\ak_scheduler.json)

## 数据链路与接口
### 股票
- 用途：股票基础信息、非复权日线、临时前复权日线
- 接口：
  - `ak.stock_info_sh_name_code`
  - `ak.stock_info_sz_name_code`
  - `ak.stock_info_bj_name_code`
  - `ak.stock_zh_a_spot`
  - `ak.stock_zh_a_hist_tx`
  - `ak.stock_zh_a_daily(adjust="qfq")`
- 写入表：
  - `stock_info_all`
  - `stock_daily_data`
  - `stock_qfq_daily_data`
- 命令：
```bash
python run.py stock daily
python run.py stock backfill
python stock_temp_service.py serve
python stock_temp_service.py health
```
- 说明：
  - `daily` 先同步三交易所股票信息，再用 `stock_zh_a_spot` 写入当日快照
  - `backfill` 先同步股票信息，再用 `stock_zh_a_spot` 获取股票池，并按上市日期调用 `stock_zh_a_hist_tx`
  - 临时采集服务按单只股票触发 `stock_zh_a_daily(adjust="qfq")`，写入 `stock_qfq_daily_data`
  - 股票编码统一同时保存两种形式：
    - `stock_code`：6 位纯代码
    - `prefixed_code`：`sh/sz/bj + stock_code`

### 指数
- 用途：指数基础信息、指数日线
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

### 量化指数看板预计算
- 用途：为 FIT 指数量化页预计算情绪、期现差、涨跌家数
- 来源：
  - `index_daily_data`
  - `excel_index_emotion_daily`
  - `futures_daily_data`
  - `stock_daily_data`
- 写入表：
  - `quant_index_dashboard_daily`
- 命令：
```bash
python run.py quant-index backfill
python run.py quant-index daily
```
- 说明：
  - 固定落 5 个指数：
    - 上证指数
    - 上证50
    - 沪深300
    - 中证500
    - 中证1000
  - 上证指数的情绪值和期现差按四大核心指数同日平均计算
  - 涨跌家数 5 个指数共用同一份值
  - `index_code` 按 FIT 当前 `/stocks/indexes/options` 口径对齐

### CFFEX 会员排名
- 用途：中金所持仓排名
- 来源：
  - [中金所会员排名页面](http://www.cffex.com.cn/ccpm/)
- 实现方式：
  - Playwright 网页抓取，不走 AKShare
- 写入表：
  - `cffex_member_rankings`
- 命令：
```bash
python run.py cffex backfill
python run.py cffex daily
```

### 外汇与美元指数
- 用途：人民币相关汇率与美元指数
- 接口：
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
- 说明：
  - 普通汇率日更默认只更新：
    - `USDCNH`
    - `CNHJPY`
    - `CNHEUR`
    - `CNHHKD`
    - `USDHKD`
    - `USDJPY`
    - `USDEUR`
  - 普通汇率日更直接使用 `forex_hist_em`
  - 美元指数走独立链路

### ETF
- 用途：ETF 基础信息、当日快照、历史日线
- 接口：
  - `ak.fund_etf_category_sina(symbol="ETF基金")`
  - `ak.fund_etf_hist_sina(symbol="sh510050")`
- 写入表：
  - `etf_basic_info_sina`
  - `etf_daily_data_sina`
- 命令：
```bash
python run.py etf daily
python run.py etf backfill
python run.py etf weekly-repair
python run.py etf repair-backfill
```
- 说明：
  - 新链路已经全部切到新浪来源
  - 旧表 `etf_basic_info`、`etf_daily_data` 保留但不再更新

### 中金所期货
- 用途：具体合约日线、主连、月连
- 接口：
  - `ak.get_futures_daily`
  - `ak.futures_hist_em` 仅保留为兼容入口
- 写入表：
  - `futures_daily_data`
- 命令：
```bash
python run.py futures backfill
python run.py futures daily
python run.py futures trade-date 2026-03-25
python run.py futures hist-backfill
python run.py futures hist-daily
```
- 说明：
  - 默认主流程先采 `get_futures_daily` 的具体合约
  - 再派生 `IF/IC/IH/IM` 的主连和月连
  - 新派生连续数据来源：
    - `get_futures_daily_derived`

### 中金所期权网页日统计
- 用途：期权日统计表
- 来源：
  - [中金所日统计页面](http://www.cffex.com.cn/rtj/)
- 实现方式：
  - Playwright 网页抓取，不走 AKShare
- 写入表：
  - `option_cffex_rtj_daily_data`
- 命令：
```bash
python run.py option backfill
python run.py option daily
python run.py option repair-backfill
```
- 说明：
  - 旧 AK 期权表 `option_cffex_spot_data`、`option_cffex_daily_data` 保留但不再更新

### 抖音情绪指标
- 来源：
  - 抖音网页播放页 + AI 总结
- 写入表：
  - `douyin_index_emotion_daily`
- 命令：
```bash
python run.py douyin backfill
python run.py douyin daily
```

### Excel 情绪指标
- 来源：
  - `pandas.read_excel`
- 写入表：
  - `excel_index_emotion_daily`
- 命令：
```bash
python run.py emotion-excel import
python run.py emotion-excel import 情绪指标.xlsx
```

## 日常采集
统一日更入口：

```bash
python run.py runner daily
```

当前包含：

- 股票 `stock_daily`
- 指数 `index_daily`
- CFFEX 会员排名 `cffex_daily`
- 外汇 `forex_daily`
- 美元指数 `usd_index_once`
- 期货 `futures_daily`
- ETF `etf_daily`
- 期权 `option_daily`
- 量化指数看板 `quant_index_daily`

当前不包含：

- 抖音情绪指标
- Excel 情绪指标导入
- ETF `weekly-repair`
- 股票临时前复权采集服务

失败任务统一重试入口：

```bash
python run.py runner retry-failures
python run.py runner retry-failures stock_daily 20
```

## 股票临时采集服务
启动：

```bash
python stock_temp_service.py serve
```

健康检查：

```bash
python stock_temp_service.py health
```

接口：

- `GET /health`
- `POST /collect`

请求示例：

```json
{
  "stock_code": "600000",
  "start_date": "2020-01-01",
  "end_date": "2026-03-27"
}
```

更详细的接入文档见：

- [docs/STOCK_TEMP_SERVICE_INTEGRATION.md](C:\Users\Administrator\PycharmProjects\akshareProkect\docs\STOCK_TEMP_SERVICE_INTEGRATION.md)

## SQL 文件
当前常用 SQL 文件：

- [sql/stock_tables.sql](C:\Users\Administrator\PycharmProjects\akshareProkect\sql\stock_tables.sql)
- [sql/index_tables.sql](C:\Users\Administrator\PycharmProjects\akshareProkect\sql\index_tables.sql)
- [sql/quant_index_tables.sql](C:\Users\Administrator\PycharmProjects\akshareProkect\sql\quant_index_tables.sql)
- [sql/cffex_tables.sql](C:\Users\Administrator\PycharmProjects\akshareProkect\sql\cffex_tables.sql)
- [sql/forex_tables.sql](C:\Users\Administrator\PycharmProjects\akshareProkect\sql\forex_tables.sql)
- [sql/etf_tables.sql](C:\Users\Administrator\PycharmProjects\akshareProkect\sql\etf_tables.sql)
- [sql/futures_tables.sql](C:\Users\Administrator\PycharmProjects\akshareProkect\sql\futures_tables.sql)
- [sql/option_rtj_tables.sql](C:\Users\Administrator\PycharmProjects\akshareProkect\sql\option_rtj_tables.sql)
- [sql/failed_task_tables.sql](C:\Users\Administrator\PycharmProjects\akshareProkect\sql\failed_task_tables.sql)
- [sql/ak_scheduler_tables.sql](C:\Users\Administrator\PycharmProjects\akshareProkect\sql\ak_scheduler_tables.sql)
- [sql/fix_all_datetime_timezone.sql](C:\Users\Administrator\PycharmProjects\akshareProkect\sql\fix_all_datetime_timezone.sql)

## 数据展示文档
给外部项目查表和展示使用：

- [docs/DATABASE_DISPLAY_GUIDE.md](C:\Users\Administrator\PycharmProjects\akshareProkect\docs\DATABASE_DISPLAY_GUIDE.md)

## 备注
- 所有 AKShare 请求必须在 [ak_scheduler_service.py](C:\Users\Administrator\PycharmProjects\akshareProkect\ak_scheduler_service.py) 启动后再执行
- 股票新链路已经不再更新旧表 `stock_basic_info`、`stock_data`
- 若数据库中存在旧的 `TIMESTAMP` 字段历史表，建议执行一次：
  - [sql/fix_all_datetime_timezone.sql](C:\Users\Administrator\PycharmProjects\akshareProkect\sql\fix_all_datetime_timezone.sql)
