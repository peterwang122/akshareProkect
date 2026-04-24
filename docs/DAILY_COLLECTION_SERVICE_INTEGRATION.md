# 日常采集服务对接说明

本文档给另一个项目使用，说明如何通过：

- `python stock_temp_service.py serve`

触发本项目已经暴露出来的日常采集任务。

## 设计原则

- 只暴露单项 HTTP 入口
- 不提供“全部日更一键总入口”
- 历史回补 `backfill-*` 不通过服务暴露
- 服务是同步 HTTP 模式，不返回后台任务 id
- 统一 daily endpoint 在 v1 只接受空 JSON：`{}`

兼容保留的特殊入口：

- `POST /collect`

它是股票 HFQ 专项接口，不走本文档定义的统一 daily 响应协议。详情见：

- `docs/STOCK_TEMP_SERVICE_INTEGRATION.md`

## 启动前置条件

先启动 AK 调度服务：

```bash
python ak_scheduler_service.py serve
```

再启动日常采集服务：

```bash
python stock_temp_service.py serve
```

健康检查：

```bash
python stock_temp_service.py health
```

默认地址：

- `http://127.0.0.1:8786`

## 健康检查

请求：

```http
GET /health
```

响应示例：

```json
{
  "status": "ok",
  "service": "stock_temp_service",
  "host": "127.0.0.1",
  "port": 8786,
  "thread": "MainThread"
}
```

## 统一 daily endpoint 响应协议

所有 daily endpoint 都接受：

```json
{}
```

### 成功响应

```json
{
  "status": "SUCCESS",
  "task_name": "stock_daily",
  "started_at": "2026-04-19 16:30:01",
  "finished_at": "2026-04-19 16:31:10",
  "duration_seconds": 69.125,
  "result": 5496
}
```

### 失败响应

HTTP 500：

```json
{
  "status": "FAILED",
  "task_name": "stock_daily",
  "error": "...",
  "started_at": "2026-04-19 16:30:01",
  "finished_at": "2026-04-19 16:30:12",
  "duration_seconds": 11.021
}
```

### 请求错误

HTTP 400：

```json
{
  "status": "INVALID_REQUEST",
  "error": "daily endpoints currently accept only an empty JSON object"
}
```

## 接口列表

### 依赖链主线

这些接口建议严格按顺序串行调用：

1. `POST /collect-stock-daily`
2. `POST /collect-index-cn-daily`
3. `POST /collect-cffex-daily`
4. `POST /collect-forex-daily`
5. `POST /collect-usd-index-daily`
6. `POST /collect-futures-daily`
7. `POST /collect-etf-daily`
8. `POST /collect-option-daily`
9. `POST /collect-quant-index-daily`

任务含义：

- `/collect-stock-daily`：股票日更，等价于 `stock.sync_daily`
- `/collect-index-cn-daily`：A 股指数日更，等价于 `index.sync_daily_from_spot`
- `/collect-cffex-daily`：中金所会员持仓日更，等价于 `cffex.sync_latest_daily_data(headless=True)`
- `/collect-forex-daily`：汇率日更，等价于 `forex.sync_daily_from_history`
- `/collect-usd-index-daily`：美元指数日更，等价于 `forex.sync_usd_index_once`
- `/collect-futures-daily`：中金所期货日更，等价于 `futures.sync_today`
- `/collect-etf-daily`：ETF 日更，等价于 `etf.sync_daily`
- `/collect-option-daily`：中金所期权日更，等价于 `option.sync_daily(headless=True)`
- `/collect-quant-index-daily`：量化指数看板日更，等价于 `quant_index.sync_daily`

固定顺序说明：

- `quant_index_daily` 依赖股票、A 股指数、期货等主链数据，必须放在主链最后
- 这 9 个接口本质上对应当前 `python run.py runner daily` 的主链任务拆分版

### 可独立调度的日更

这些接口当前不反向影响 `quant_index_dashboard_daily`，可以按需单独调用，也可以与主链并行：

- `POST /collect-index-us-daily`
- `POST /collect-index-hk-daily`
- `POST /collect-index-qvix-daily`
- `POST /collect-index-news-sentiment-daily`
- `POST /collect-index-us-vix-daily`
- `POST /collect-index-us-fear-greed-daily`
- `POST /collect-index-us-hedge-proxy-daily`
- `POST /collect-us-index-futures-daily`
- `POST /collect-hk-index-futures-daily`

任务含义：

- `/collect-index-us-daily`：US 指数日更
- `/collect-index-hk-daily`：HK 指数日更
- `/collect-index-qvix-daily`：5 条 QVIX 序列最新日线
- `/collect-index-news-sentiment-daily`：新闻情绪范围最新一行
- `/collect-index-us-vix-daily`：美股 VIX 最新 OHLC
- `/collect-index-us-fear-greed-daily`：美股恐贪指数最新值
- `/collect-index-us-hedge-proxy-daily`：ES / NQ 两条对冲基金多空代理最新可得报告
- `/collect-us-index-futures-daily`：美股股指期货日更，维护 `ES` / `NQ` 连续/品种级日线，数据源为新浪外盘期货
- `/collect-hk-index-futures-daily`：港股股指期货日更，维护 `HSI` / `HHI` / `HTI` 具体月份合约，数据源为 HKEX 官方源

## 可复制调用示例

### PowerShell 示例

股票日更：

```powershell
Invoke-RestMethod -Method Post `
  -Uri "http://127.0.0.1:8786/collect-stock-daily" `
  -ContentType "application/json" `
  -Body '{}'
```

A 股指数日更：

```powershell
Invoke-RestMethod -Method Post `
  -Uri "http://127.0.0.1:8786/collect-index-cn-daily" `
  -ContentType "application/json" `
  -Body '{}'
```

量化看板日更：

```powershell
Invoke-RestMethod -Method Post `
  -Uri "http://127.0.0.1:8786/collect-quant-index-daily" `
  -ContentType "application/json" `
  -Body '{}'
```

US 指数日更：

```powershell
Invoke-RestMethod -Method Post `
  -Uri "http://127.0.0.1:8786/collect-index-us-daily" `
  -ContentType "application/json" `
  -Body '{}'
```

美股 VIX 日更：

```powershell
Invoke-RestMethod -Method Post `
  -Uri "http://127.0.0.1:8786/collect-index-us-vix-daily" `
  -ContentType "application/json" `
  -Body '{}'
```

美股股指期货日更：

```powershell
Invoke-RestMethod -Method Post `
  -Uri "http://127.0.0.1:8786/collect-us-index-futures-daily" `
  -ContentType "application/json" `
  -Body '{}'
```

说明：该接口采集新浪外盘期货 `ES` / `NQ` 连续/品种级日线，`contract_month` 固定为 `CONTINUOUS`，不是具体月份合约。

港股股指期货日更：

```powershell
Invoke-RestMethod -Method Post `
  -Uri "http://127.0.0.1:8786/collect-hk-index-futures-daily" `
  -ContentType "application/json" `
  -Body '{}'
```

### Python 示例

```python
import requests


def call_daily_endpoint(path: str):
    response = requests.post(
        f"http://127.0.0.1:8786{path}",
        json={},
        timeout=7200,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("status") != "SUCCESS":
        raise RuntimeError(payload)
    return payload


result = call_daily_endpoint("/collect-stock-daily")
print(result)
```

### Celery task 示例

```python
from celery import shared_task
import requests


def _call(path: str):
    response = requests.post(
        f"http://127.0.0.1:8786{path}",
        json={},
        timeout=7200,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("status") != "SUCCESS":
        raise RuntimeError(payload)
    return payload


@shared_task(
    bind=True,
    name="tasks.collect_stock_daily",
    autoretry_for=(requests.RequestException,),
    retry_backoff=True,
    retry_kwargs={"max_retries": 5},
)
def collect_stock_daily_task(self):
    return _call("/collect-stock-daily")
```

## 重试与超时建议

- 主链接口建议串行调用
- 主链单项建议超时：`1h - 2h`
- 独立接口可按任务重量单独设置更短超时

建议重试场景：

- HTTP 5xx
- 网络超时
- 连接失败

建议直接失败、不自动重试：

- HTTP 400
- 请求体不是 JSON
- daily endpoint 传了非空参数对象

## 不通过服务暴露的能力

以下能力仍然只通过本地命令执行：

- 所有 `backfill-*`
- `python run.py douyin daily`
- `python run.py emotion-excel import`

## `/collect` 兼容说明

`POST /collect` 继续保留，用于股票 HFQ 专项刷新：

- 请求体不是空 JSON，而是股票代码和日期区间
- 响应协议不是本文档的统一 daily 协议
- 详见 `docs/STOCK_TEMP_SERVICE_INTEGRATION.md`
