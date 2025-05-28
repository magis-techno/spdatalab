# Spatial‑Data‑Lab 项目指南

本仓库是一个 **最小可运行** 的空间数据分析脚手架，集成：

* **OBS → 本地**：moxing 下载
* **多 Hive catalog 查询**：业务库 / 轨迹库 / RoadCode / Tag 数据湖
* **本地 PostGIS + FDW**：快速叠置分析
* **Docker 一键启动**：开发环境与云端镜像一致
* **GeoPackage 输出**：离线抽查 + QGIS 浏览
* **VS Code Remote‑Containers 断点调试**

---

## 目录结构

```text
<repo>/
├─ .env.example           # 复制为 .env 并填写凭证
├─ docker/                # Dockerfile & compose
├─ sql/                   # 建表 & FDW 脚本
├─ src/spdatalab/         # Python 包
├─ tests/                 # pytest 用例
└─ data/                  # 样例数据／输出
```

---

## 快速开始

```bash
# 1. 克隆代码并填写凭证
cp .env.example .env

# 2. 启动容器（PostGIS + workspace）
make up

# 3. 初始化本地数据库
make init-db

# 4. 进入 workspace 容器
docker exec -it workspace bash

# 5. 安装项目（可 hot‑reload）
pip install -e .

# 6. 运行样例 ETL
python -m spdatalab.dataset.ingest \
       --list data/sample_list.txt \
       --out  data/out

# 7. 在 QGIS 连接 localhost:5432 抽查结果
# 8. 结束开发
make down
```

---

## Hive 多 Catalog 用法

```python
from spdatalab.common.io_hive import hive_cursor

# 业务库：scene_token ↔ dataset_name
with hive_cursor('app_gy1') as cur:
    cur.execute("SELECT ...")

# 轨迹库
with hive_cursor('dataset_gy1') as cur:
    ...

# RoadCode / Tag 数据湖同理
```

---

## 调试 · FAQ

| 目的 | 命令 |
|------|------|
| 单元测试 | `pytest -q` |
| IPython 交互 | `python -m ipython` |
| VS Code 断点 | Remote‑Containers 搭建后 `F5` |
| Jupyter | `jupyter lab --ip 0.0.0.0` |
| 容器热更新依赖 | `docker compose build workspace` |

常见报错：

* `ModuleNotFoundError` → 容器内未 `pip install -e .`
* QGIS 连接失败 → 检查 `docker ps` 端口映射
* pip 超时外网 → 已强制使用公司内源，不应访问 pypi.org

---

## 生产部署

1. 将 `docker/Dockerfile` 推送制品库，云端 `docker run` 即可。
2. Airflow/Dagster DAG 调用 `spdatalab.fusion.*` 模块完成批量叠置。
3. 输出 `result_YYYYMMDD.gpkg` 上传对象存储供下游消费。

---

> 有新的分析需求？  
> 只需在 `src/spdatalab/<domain>/` 新增模块，写单测，CI 自动运行。