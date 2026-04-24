# 股票 HFQ 临时采集专项说明

本文件只描述股票后复权专项接口：

- `POST /collect`

如果你要对接本项目统一的日常采集 HTTP 服务，请优先阅读：

- `docs/DAILY_COLLECTION_SERVICE_INTEGRATION.md`

## 用途

`/collect` 用于按单只股票、按指定区间触发后复权历史刷新，结果写入：

- `stock_hfq_daily_data`

这个接口保留为兼容入口，不属于统一 daily endpoint 协议。

## 启动方式

先启动 AK 调度服务：

```bash
python ak_scheduler_service.py serve
```

再启动临时服务：

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
  "end_date": "2026-04-19"
}
```

字段说明：

- `stock_code`：必填，6 位股票代码
- `start_date`：可选，格式 `YYYY-MM-DD`
- `end_date`：可选，格式 `YYYY-MM-DD`

## 服务规则

- 先用 `stock_code` 查询 `stock_info_all` 上市日期
- 查不到上市日期时回退到 `1991-01-01`
- `effective_start_date = max(request.start_date, 上市日期/1991-01-01)`
- `effective_end_date = request.end_date 或今日`
- 数据源为 `stock_zh_a_daily(adjust="hfq")`

幂等规则：

- 如果本次区间与该股票最近一次刷新区间完全一致，直接返回 `UNCHANGED`
- 如果区间变化，则先删除该股票在 `stock_hfq_daily_data` 中的旧记录，再整段重写

## 响应示例

成功刷新：

```json
{
  "status": "SUCCESS",
  "stock_code": "600000",
  "prefixed_code": "sh600000",
  "effective_start_date": "2020-01-01",
  "effective_end_date": "2026-04-19",
  "refreshed": true,
  "unchanged": false,
  "deleted_rows": 1200,
  "written_rows": 1450
}
```

区间未变化：

```json
{
  "status": "UNCHANGED",
  "stock_code": "600000",
  "prefixed_code": "sh600000",
  "effective_start_date": "2020-01-01",
  "effective_end_date": "2026-04-19",
  "refreshed": false,
  "unchanged": true,
  "deleted_rows": 0,
  "written_rows": 0
}
```

请求错误：

```json
{
  "status": "INVALID_REQUEST",
  "error": "stock_code must be a 6-digit code"
}
```

服务内部错误：

```json
{
  "status": "FAILED",
  "error": "..."
}
```

## 外部项目接入建议

- 每次只让一个任务对应一次 `POST /collect`
- HTTP 200 且 `status` 为 `SUCCESS` 或 `UNCHANGED` 都视为成功
- HTTP 5xx、网络超时、连接失败建议重试
- HTTP 400 这类参数错误建议直接失败，不自动重试

PowerShell 示例：

```powershell
Invoke-RestMethod -Method Post `
  -Uri "http://127.0.0.1:8786/collect" `
  -ContentType "application/json" `
  -Body '{"stock_code":"600000","start_date":"2020-01-01","end_date":"2026-04-19"}'
```
