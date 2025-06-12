# Mock数据方案

这个目录包含了完整的Mock数据解决方案，用于在本地开发和测试环境中模拟远程数据库。

## 架构概述

Mock系统包含3个数据库：
1. **轨迹数据库** (trajectory_db) - 存储轨迹点数据
2. **业务数据库** (business_db) - 存储元数据信息  
3. **地图数据库** (map_db) - 存储路口等地图数据

## 目录结构

```
mock/
├── README.md                   # 本文档
├── docker-compose.mock.yml     # Mock服务的Docker配置
├── databases/                  # 数据库初始化脚本
│   ├── trajectory/             # 轨迹数据库
│   ├── business/              # 业务数据库
│   └── map/                   # 地图数据库
├── data_generators/           # 测试数据生成器
│   ├── trajectory_generator.py # 轨迹数据生成
│   ├── business_generator.py  # 业务数据生成
│   ├── map_generator.py       # 地图数据生成
│   └── dataset_generator.py   # 数据集文件生成
├── test_data/                 # 静态测试数据
│   ├── datasets/              # 数据集txt文件
│   ├── jsonl/                 # JSONL子数据集文件
│   └── samples/               # 示例数据
└── scripts/                   # 管理脚本
    ├── setup_mock.py          # Mock环境初始化
    ├── reset_data.py          # 重置测试数据
    └── validate_mock.py       # 验证Mock数据
```

## 快速开始

1. **启动Mock环境**:
   ```bash
   make mock-up
   ```

2. **初始化测试数据**:
   ```bash
   make mock-init
   ```

3. **验证Mock环境**:
   ```bash
   make mock-check
   ```

## 数据库配置

### 轨迹数据库 (trajectory_db)
- 端口: 5433
- 数据库: trajectory  
- 表: ddi_data_points
- 主要字段: point_lla (geometry), dataset_name, workstage

### 业务数据库 (business_db)  
- 端口: 5434
- 数据库: business
- 表: ods_t_data_fragment_datalake
- 主要字段: id, origin_name, event_id, city_id, timestamp

### 地图数据库 (map_db)
- 端口: 5435  
- 数据库: mapdb
- 表: full_intersection
- 主要字段: id, intersectiontype, intersectionsubtype, wkb_geometry

## 使用说明

Mock环境启动后，修改你的配置文件连接到本地Mock数据库：

```python
# 轨迹数据库连接
TRAJECTORY_DSN = "postgresql+psycopg://postgres:postgres@localhost:5433/trajectory"

# 业务数据库连接 
BUSINESS_DSN = "postgresql+psycopg://postgres:postgres@localhost:5434/business"

# 地图数据库连接
MAP_DSN = "postgresql+psycopg://postgres:postgres@localhost:5435/mapdb"
```

## 测试数据生成

系统提供了灵活的测试数据生成器，可以根据需要生成不同规模和特征的测试数据：

- **小规模测试**: 100个场景，10个路口
- **中规模测试**: 1000个场景，100个路口  
- **大规模测试**: 10000个场景，1000个路口

## 管理命令

| 命令 | 说明 |
|------|------|
| `make mock-up` | 启动Mock环境 |
| `make mock-down` | 停止Mock环境 |
| `make mock-init` | 初始化测试数据 |
| `make mock-reset` | 重置所有数据 |
| `make mock-check` | 验证Mock环境 |
| `make mock-logs` | 查看Mock服务日志 | 