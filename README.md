# akshareProkect

一个基于 **AKShare + asyncio + aiomysql** 的 A 股历史行情采集与入库工具。  
程序会读取股票列表，抓取每只股票从上市日至今天的日线数据（后复权），并批量写入 MySQL。

---

## 1. 项目能力

- 自动读取股票代码并标准化（保留前导 0，如 `000356`）。
- 调用 AKShare 获取：
  - 上市时间：`stock_individual_info_em`
  - 历史日线：`stock_zh_a_hist`
- 内置失败重试机制，降低接口偶发失败影响。
- 使用 `asyncio` 控制并发抓取，提高处理效率。
- 使用 `aiomysql` 连接池 + `executemany` 进行批量入库。
- 入库前做数值清洗（NaN/Inf/越界），避免 MySQL 1264 错误。
- 通过 `progress.log` 支持断点续跑。
- 通过 `error.log` 记录失败股票与错误信息。

---

## 2. 目录结构

```text
akshareProkect/
├── main.py                  # 主流程：读股票、抓数据、批量入库
├── requirements.txt         # 依赖
├── allstock_em.csv          # 股票清单（需包含“代码”列）
├── progress.log             # 进度日志（运行时生成）
├── error.log                # 错误日志（运行时生成）
├── config/
│   ├── get_config_path.py
│   └── db_info.json         # 数据库配置（需自行创建）
└── util/
    └── db_tool.py           # aiomysql 连接池、数据清洗、批量写库
```

---

## 3. 环境要求

- Python 3.9+
- MySQL 5.7 / 8.0
- 可访问 AKShare 数据源的网络

---

## 4. 安装

```bash
pip install -r requirements.txt
```

---

## 5. 配置

### 5.1 股票列表文件

在项目根目录准备 `allstock_em.csv`，至少包含 `代码` 列：

```csv
代码
000001
000002
600519
832000
```

> 建议把 `代码` 列作为字符串保存，避免前导零丢失。

### 5.2 数据库连接

在 `config/db_info.json` 中配置 MySQL：

```json
{
  "host": "127.0.0.1",
  "port": 3306,
  "user": "root",
  "password": "your_password",
  "database": "your_database",
  "charset": "utf8mb4"
}
```

---

## 6. 数据表要求

程序默认写入 `stock_data` 表，使用字段：

- `stock_code`
- `open_price`
- `close_price`
- `high_price`
- `low_price`
- `volume`
- `turnover`
- `amplitude`
- `price_change_rate`
- `price_change_amount`
- `turnover_rate`
- `date`

建议建立联合唯一索引：`(stock_code, date)`。

参考建表 SQL（可按你生产库规范调整精度）：

```sql
CREATE TABLE IF NOT EXISTS stock_data (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  stock_code VARCHAR(16) NOT NULL,
  open_price DECIMAL(12,4) NULL,
  close_price DECIMAL(12,4) NULL,
  high_price DECIMAL(12,4) NULL,
  low_price DECIMAL(12,4) NULL,
  volume DECIMAL(20,2) NULL,
  turnover DECIMAL(22,2) NULL,
  amplitude DECIMAL(8,4) NULL,
  price_change_rate DECIMAL(8,4) NULL,
  price_change_amount DECIMAL(12,4) NULL,
  turnover_rate DECIMAL(8,4) NULL,
  date DATE NOT NULL,
  UNIQUE KEY uk_stock_date (stock_code, date)
);
```

---

## 7. 运行

```bash
python main.py
```

执行过程：

1. 读取 `allstock_em.csv`
2. 并发抓取股票信息和历史行情
3. 过滤已处理进度
4. 批量写入数据库
5. 更新 `progress.log` / `error.log`

---

## 8. 可调参数（`main.py`）

- `API_RETRY_COUNT`：AKShare 请求失败最大重试次数（默认 5）
- `API_RETRY_SLEEP_SECONDS`：重试间隔秒数（默认 3）
- `MAX_CONCURRENCY`：并发股票处理数（默认 8）

调优建议：

- 如果经常触发限流：降低 `MAX_CONCURRENCY`，提高重试间隔。
- 如果机器资源充足且网络稳定：可适当提高并发。

---

## 9. 日志说明

### progress.log

- 格式：`stock_code,date`
- 用于断点续跑，程序会跳过已完成记录。

### error.log

- 格式：`stock_code,date,error_message`
- 常见问题：
  - AKShare 接口限流或网络异常
  - 数据库连接配置错误
  - 字段值异常（程序已做统一清洗）

---

## 10. 常见问题

### Q1：为什么有些股票没有入库？
可能原因：

- 股票代码无效或数据源不存在对应数据
- 接口在多次重试后仍失败
- 数据全部在 `progress.log` 中已被标记完成

### Q2：出现 Out of range 报错怎么办？

当前代码已对异常值进行清洗并转为 `NULL`，如仍出现，建议同步检查：

- `stock_data` 表字段精度是否过小
- 是否有触发器/约束导致插入失败

---

## 11. 后续优化建议

- 增加增量更新模式（例如仅更新最近 N 天）。
- 增加命令行参数（并发、重试、输入文件路径）。
- 增加单元测试与集成测试。
- 将配置统一迁移到 `.env` 或 YAML 配置文件。
