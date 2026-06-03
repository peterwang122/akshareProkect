# 本机 Docker MySQL 迁移说明

## 目标结构

- FIT 项目负责启动本机 Docker 基础设施：MySQL、Redis、Flower。
- MySQL 数据库名统一为 `stock_info`，Docker 命名 volume `mysql_data` 持久化 `/var/lib/mysql`。
- `akshareProkect` 写入本机 Docker MySQL。
- FIT 后端直连本机 Docker MySQL，FIT 前端只访问 FIT 后端 API。

## 首次准备

在 FIT 项目：

```bash
cp .env.example .env
cp backend/.env.example backend/.env
conda env create -f environment.yml
conda activate FIT
python scripts/dev.py infra
cd frontend
npm install
```

在 akshareProkect 项目：

```bash
conda env create -f environment.yml
conda activate akshareProkect
python -m playwright install chromium
```

## Windows 到 Mac 一次性切换

1. 停止 Windows 端所有写库程序：采集、AK scheduler、stock temp service、FIT、Celery、定时任务。
2. 确认 Mac 可以访问 Windows MySQL：`192.168.1.16:3306`。
3. 在 akshareProkect 执行：

```bash
REMOTE_DB_PASSWORD=... python scripts/db_sync.py windows-to-mac --confirm
REMOTE_DB_PASSWORD=... python scripts/db_compare.py
```

脚本会先备份 Mac 目标库，再覆盖恢复 `stock_info`。

## Mac 到 Windows 手动备份

Mac 成为主端后，如果需要把本机库备份到 Windows 备用库，手动执行：

```bash
REMOTE_DB_PASSWORD=... python scripts/db_sync.py mac-to-windows --confirm
```

这个命令不会被 launchd、cron 或 Celery beat 自动调用。

## 本机启动

在 akshareProkect：

```bash
conda activate akshareProkect
python scripts/dev.py scheduler
python scripts/dev.py stock-temp
```

在 FIT：

```bash
conda activate FIT
python scripts/dev.py api
python scripts/dev.py worker
python scripts/dev.py beat
python scripts/dev.py frontend
```

macOS 后台托管可按需安装：

```bash
python scripts/install_launchd.py scheduler stock-temp
cd ../FIT
python scripts/install_launchd.py api worker beat
```

## 验证

- `python scripts/dev.py scheduler-health`
- `python scripts/dev.py stock-temp-health`
- FIT API: `http://127.0.0.1:8000`
- FIT 前端: `http://127.0.0.1:5173`
- Flower: `http://127.0.0.1:5555`
- Docker MySQL 重启后 `stock_info` 数据仍存在。
