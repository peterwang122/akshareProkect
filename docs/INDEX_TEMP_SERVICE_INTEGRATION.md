# 指数临时采集专项说明

本文件保留为指数专项说明，帮助区分两类能力：

- 统一日常采集 HTTP 入口：`docs/DAILY_COLLECTION_SERVICE_INTEGRATION.md`
- 本地命令式历史回补：`python run.py index ...`

如果另一个项目要通过 HTTP 触发本项目的日常采集，请优先阅读：

- `docs/DAILY_COLLECTION_SERVICE_INTEGRATION.md`

## 通过服务暴露的指数日更

当前 `python stock_temp_service.py serve` 会暴露这些指数日更接口：

- `POST /collect-index-cn-daily`
- `POST /collect-index-us-daily`
- `POST /collect-index-hk-daily`
- `POST /collect-index-qvix-daily`
- `POST /collect-index-news-sentiment-daily`
- `POST /collect-index-us-vix-daily`
- `POST /collect-index-us-fear-greed-daily`
- `POST /collect-index-us-hedge-proxy-daily`

这些接口的统一响应协议、推荐调用顺序、重试建议都在总文档里统一维护，不在本文件重复展开。

## 不通过服务暴露的指数历史回补

以下能力仍然只通过本地命令执行，不对外暴露 HTTP 接口：

- `python run.py index backfill`
- `python run.py index backfill-bj899050`
- `python run.py index backfill-us`
- `python run.py index backfill-hk`
- `python run.py index backfill-qvix`
- `python run.py index backfill-news-sentiment`
- `python run.py index backfill-us-vix`
- `python run.py index backfill-us-fear-greed`
- `python run.py index backfill-us-hedge-proxy`
- `python run.py index backfill-us-market-sentiment`

## 口径补充

### A 股指数

- `index daily` 主链走 `stock_zh_index_spot_sina`
- `bj899050` 为特例，日更与历史都单独走 `stock_zh_index_daily`

### US/HK 指数

- US 日更接口：`/collect-index-us-daily`
- HK 日更接口：`/collect-index-hk-daily`
- 两者都已经纳入统一日常采集服务，不需要另做专项服务

### QVIX / 新闻情绪 / 美股风险情绪

- QVIX、新闻情绪、美股 VIX、恐贪指数、对冲基金多空代理都已经有独立日更 HTTP 入口
- 但它们的历史回补仍然只通过 `python run.py index ...` 命令完成
