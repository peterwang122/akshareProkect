# 股票临时采集服务对接说明

本文档给另一个项目使用，说明如何调用本项目提供的股票前复权临时采集服务。

## 服务用途
该服务用于按单只股票触发前复权日线采集，并把结果写入本项目数据库中的：

- `stock_qfq_daily_data`

服务是同步 HTTP 模式，适合被 Celery task 直接调用。

## 启动方式
先启动 AK 调度服务：

```bash
python ak_scheduler_service.py serve
```

再启动股票临时采集服务：

```bash
python stock_temp_service.py serve
```

健康检查：

```bash
python stock_temp_service.py health
```

默认地址：

- `http://127.0.0.1:8786`

## HTTP 接口
### 1. 健康检查
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

### 2. 触发采集
请求：

```http
POST /collect
Content-Type: application/json
```

请求体：

```json
{
  "stock_code": "600000",
  "start_date": "2020-01-01",
  "end_date": "2026-03-27"
}
```

字段说明：

- `stock_code`：必填，6 位股票代码
- `start_date`：可选，格式 `YYYY-MM-DD`
- `end_date`：可选，格式 `YYYY-MM-DD`

服务内部规则：

- 先按 `stock_code` 查询 `stock_info_all` 的上市日期
- 若查不到上市日期，则回退到 `1991-01-01`
- `effective_start_date = 请求 start_date 或 上市日期/1991-01-01`
- `effective_end_date = 请求 end_date 或 今日`
- 使用 `stock_zh_a_daily(adjust="qfq")` 拉取从起始到结束的前复权数据

## 幂等规则
服务会读取 `stock_qfq_daily_data` 中该股票最近一次刷新记录的：

- `request_start_date`
- `request_end_date`

如果本次请求区间与上次完全一致，则：

- 不会重新删除和写入数据
- 直接返回 `UNCHANGED`

如果区间不同，则：

- 先删除该股票在 `stock_qfq_daily_data` 中的全部历史
- 再全量写入新的前复权数据

## 响应格式
### 成功刷新
```json
{
  "status": "SUCCESS",
  "stock_code": "600000",
  "prefixed_code": "sh600000",
  "effective_start_date": "2020-01-01",
  "effective_end_date": "2026-03-27",
  "refreshed": true,
  "unchanged": false,
  "deleted_rows": 1200,
  "written_rows": 1450
}
```

### 区间未变化
```json
{
  "status": "UNCHANGED",
  "stock_code": "600000",
  "prefixed_code": "sh600000",
  "effective_start_date": "2020-01-01",
  "effective_end_date": "2026-03-27",
  "refreshed": false,
  "unchanged": true,
  "deleted_rows": 0,
  "written_rows": 0
}
```

### 请求错误
HTTP 400：

```json
{
  "status": "INVALID_REQUEST",
  "error": "stock_code must be a 6-digit code"
}
```

### 服务内部错误
HTTP 500：

```json
{
  "status": "FAILED",
  "error": "..."
}
```

## Celery 对接建议
建议做法：

- 一个 Celery task 对应一次 `POST /collect`
- 用 HTTP 调用，而不是直接连本项目数据库或直接调 AKShare
- 让 Celery 只关心服务响应，不关心本项目内部的 AK 调度细节

### 成功判定
以下情况都视为任务成功：

- HTTP 200 且 `status == "SUCCESS"`
- HTTP 200 且 `status == "UNCHANGED"`

以下情况建议重试：

- HTTP 5xx
- 网络超时
- 连接失败

以下情况建议直接失败，不自动重试：

- HTTP 400
- `stock_code` 非法
- 日期区间非法

### 推荐 Celery 重试策略
- `max_retries = 5`
- `countdown = 60, 120, 300...`
- `soft_time_limit = 1800`
- `time_limit = 2100`

### Python 调用示例
```python
import requests


def collect_stock_qfq(stock_code: str, start_date: str | None = None, end_date: str | None = None):
    payload = {
        "stock_code": stock_code,
        "start_date": start_date,
        "end_date": end_date,
    }
    response = requests.post(
        "http://127.0.0.1:8786/collect",
        json=payload,
        timeout=1800,
    )
    response.raise_for_status()
    data = response.json()
    if data.get("status") not in {"SUCCESS", "UNCHANGED"}:
        raise RuntimeError(f"unexpected service status: {data}")
    return data
```

### Celery task 示例
```python
from celery import shared_task
import requests


@shared_task(bind=True, autoretry_for=(requests.RequestException,), retry_backoff=True, retry_kwargs={"max_retries": 5})
def collect_stock_qfq_task(self, stock_code, start_date=None, end_date=None):
    response = requests.post(
        "http://127.0.0.1:8786/collect",
        json={
            "stock_code": stock_code,
            "start_date": start_date,
            "end_date": end_date,
        },
        timeout=1800,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("status") not in {"SUCCESS", "UNCHANGED"}:
        raise RuntimeError(payload)
    return payload
```

## 数据读取建议
采集完成后，对方项目读取：

- `stock_qfq_daily_data`

过滤条件建议：

- `prefixed_code = 'sh600000'`
  或
- `stock_code = '600000'`

排序建议：

```sql
ORDER BY trade_date ASC
```

如果对方项目只关心最近一次刷新区间，可以同时读取：

- `request_start_date`
- `request_end_date`
- `refresh_batch_id`

## 注意事项
- 服务依赖 AK scheduler，未启动时 `POST /collect` 会失败
- 同一股票不同区间会触发整表重刷，这是为了保证前复权数据一致性
- 该服务不返回异步 job id，接口是同步的；Celery 直接等待 HTTP 结果即可  
