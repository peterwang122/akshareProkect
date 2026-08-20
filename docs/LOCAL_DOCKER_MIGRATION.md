# 本机 Docker MySQL 迁移说明
本迁移与启动说明针对正式机（Mac）执行；Windows 开发机只做代码编辑、分支、Mock/单元测试和静态检查，不启动任何服务、不连接生产资源。

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

## 正式机（Mac）启动

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
