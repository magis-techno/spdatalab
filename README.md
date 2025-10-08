# SPDataLab - 空间数据处理工具包

SPDataLab是一个专业的空间数据处理工具包，提供从数据集构建到边界框处理的完整工作流程。项目集成了Docker化的PostGIS环境，支持大规模数据处理和智能进度跟踪。

## ✨ 核心特性

- **🗄️ 数据集管理系统**：结构化管理包含数百万场景的大型数据集
- **📍 边界框处理**：高效提取和处理场景边界框数据，支持智能缓冲区
- **⚡ 进度跟踪与恢复**：智能断点续传，大规模数据处理零数据丢失
- **🐳 Docker集成环境**：PostGIS + Python环境，一键启动开发环境
- **🔄 多格式支持**：支持JSON/Parquet格式，针对大数据集优化
- **🌐 多Hive目录支持**：业务库/轨迹库/RoadCode/Tag数据湖集成
- **🔍 空间连接分析**：专业的空间数据处理和几何分析

## 🚀 快速开始

### 环境配置

```bash
# 1. 克隆项目并配置环境
git clone <repository-url>
cd spdatalab
cp .env.example .env  # 填写必要的凭证信息

# 2. 启动Docker环境
make up

# 3. 初始化数据库
make init-db

# 4. 进入工作容器
docker exec -it workspace bash

# 5. 安装项目
pip install -e .
```

### 基本使用

```bash
# 构建数据集
python -m spdatalab.cli build-dataset \
  --index-file data/index.txt \
  --dataset-name "my_dataset" \
  --output dataset.parquet \
  --format parquet

# 处理边界框（支持大规模数据和智能恢复）
python -m spdatalab.cli process-bbox \
  --input dataset.parquet \
  --batch 1000 \
  --work-dir ./logs/bbox_processing

# 两阶段完整工作流程
# 第一阶段：构建数据集
python -m spdatalab.cli build_dataset \
  --input data/index.txt \
  --dataset-name "complete_dataset" \
  --output complete_dataset.parquet \
  --format parquet

# 第二阶段：处理边界框（默认启用分表模式）
python -m spdatalab.cli process_bbox \
  --input complete_dataset.parquet \
  --batch 1000
```

## 📁 项目结构

```text
spdatalab/
├── README.md                 # 项目主文档
├── docs/                     # 详细文档目录
│   ├── README.md            # 文档导航
│   ├── cli_usage_guide.md   # CLI使用指南
│   ├── dataset_management.md # 数据集管理
│   ├── bbox_integration_guide.md # 边界框处理
│   ├── progress_tracking_guide.md # 进度跟踪
│   ├── spatial_join.md      # 空间连接
│   └── infrastructure_guide.md # 基础设施
├── src/spdatalab/           # Python包源码
├── docker/                  # Docker配置
├── sql/                     # 数据库脚本
├── tests/                   # 测试用例
├── data/                    # 示例数据
└── examples/                # 使用示例
```

## 🛠️ 主要功能

### 1. 数据集管理 📊

**支持格式**：
- **JSON格式**：适合小到中型数据集（< 10万场景）
- **Parquet格式**：大型数据集首选（400万+场景，比JSON小80-90%）

**核心功能**：
```bash
# 从索引文件构建数据集
python -m spdatalab.cli build-dataset \
  --index-file data/train_index.txt \
  --dataset-name "training_v1" \
  --output output/train_dataset.parquet \
  --format parquet

# 查看数据集信息
python -m spdatalab.cli dataset-info \
  --dataset-file output/train_dataset.parquet

# 导出场景ID
python -m spdatalab.cli list-scenes \
  --dataset-file output/train_dataset.parquet \
  --output scene_ids.txt
```

### 2. 边界框处理 📍

**高级特性**：
- 智能进度跟踪和断点续传
- 失败记录与选择性重试
- 支持大规模数据处理（400万+场景）
- 高效的Parquet格式状态存储

**使用示例**：
```bash
# 大规模数据处理
python -m spdatalab.cli process-bbox \
  --input output/large_dataset.parquet \
  --batch 1000 \
  --insert-batch 1000 \
  --work-dir ./logs/large_import_$(date +%Y%m%d)

# 查看处理统计
python -m spdatalab.cli process-bbox \
  --input output/dataset.parquet \
  --show-stats \
  --work-dir ./logs/previous_import

# 重试失败数据
python -m spdatalab.cli process-bbox \
  --input output/dataset.parquet \
  --retry-failed \
  --work-dir ./logs/previous_import
```

### 3. 空间数据分析 🗺️

```bash
# 空间连接分析
python -m spdatalab.cli spatial-join \
  --right-table intersections \
  --num-bbox 2000

# 空间查询和处理
# 详细功能请参考空间连接指南
```

### 4. 多数据源集成 🔄

```python
from spdatalab.common.io_hive import hive_cursor

# 业务库查询
with hive_cursor('app_gy1') as cur:
    cur.execute("SELECT scene_token FROM scene_table WHERE ...")

# 轨迹库查询
with hive_cursor('dataset_gy1') as cur:
    cur.execute("SELECT * FROM trajectory_table WHERE ...")
```

## 📚 详细文档

| 文档 | 说明 |
|------|------|
| **[完整文档](./docs/README.md)** | 项目文档导航和概览 |
| **[CLI使用指南](./docs/cli_usage_guide.md)** | 命令行工具完整教程 |
| **[数据集管理](./docs/dataset_management.md)** | 数据集构建和管理详解 |
| **[边界框处理](./docs/bbox_integration_guide.md)** | 边界框处理专门指南 |
| **[进度跟踪](./docs/progress_tracking_guide.md)** | 大规模处理和故障恢复 |
| **[空间连接](./docs/spatial_join.md)** | 空间数据处理和分析 |
| **[基础设施](./docs/infrastructure_guide.md)** | 环境搭建和部署指南 |

## 🔧 开发调试

| 目的 | 命令 |
|------|------|
| 单元测试 | `pytest -q` |
| 交互式开发 | `python -m ipython` |
| VS Code 调试 | Remote-Containers + `F5` |
| Jupyter 分析 | `jupyter lab --ip 0.0.0.0` |
| 热更新依赖 | `docker compose build workspace` |

## 💡 使用建议

- **新用户**：从[CLI使用指南](./docs/cli_usage_guide.md)开始
- **大数据处理**：使用Parquet格式，参考[进度跟踪指南](./docs/progress_tracking_guide.md)
- **空间分析**：查看[空间连接指南](./docs/spatial_join.md)
- **生产部署**：参考[基础设施指南](./docs/infrastructure_guide.md)

## 🚀 生产部署

1. **容器化部署**：将`docker/Dockerfile`推送到制品库，使用`docker run`部署
2. **批量处理**：使用`python -m spdatalab.cli`进行大规模数据处理
3. **监控和恢复**：利用进度跟踪系统实现零数据丢失的大规模处理

## 📈 性能特点

- **大规模支持**：单次处理400万+场景数据
- **高效存储**：Parquet格式比JSON减少80-90%存储空间
- **智能恢复**：程序中断后自动断点续传
- **内存优化**：支持流式处理，内存占用可控

## 🔗 相关链接

- **项目定位**：专业的空间数据处理工具包
- **技术栈**：Python + PostGIS + Docker + Pandas/GeoPandas
- **应用场景**：大规模空间数据处理、边界框分析、数据集管理

---

> 💡 **提示**：详细的使用方法和最佳实践请查看`./docs/`目录下的完整文档