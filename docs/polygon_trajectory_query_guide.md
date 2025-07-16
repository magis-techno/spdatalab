# 高性能Polygon轨迹查询功能使用指南

## 功能概述

`polygon_trajectory_query`模块提供了**高性能**的GeoJSON polygon轨迹查询功能，基于`spatial_join_production.py`的优化策略。该模块能够：

### 🚀 核心功能
1. 读取GeoJSON文件中的polygon（支持多个polygon）
2. **智能批量查询**与polygon相交的轨迹点
3. 将轨迹点按dataset_name连成轨迹线（LineString）
4. 计算详细的轨迹统计信息（参照trajectory.py的字段结构）
5. **高效批量写入**数据库表，支持GeoJSON导出

### ⚡ 性能特性
- **智能查询策略**：≤50个polygon使用UNION ALL批量查询，>50个polygon使用分块查询
- **批量数据库操作**：事务保护，批量插入，多重索引
- **内存优化**：分批处理，避免内存溢出
- **详细性能统计**：查询时间、构建时间、处理速度等

### 📊 性能基准
- **小规模**（≤50个polygon）：批量查询，通常<5秒
- **中规模**（50-200个polygon）：分块查询，~10-30秒  
- **大规模**（>200个polygon）：智能分块，保持稳定性能

## 轨迹字段结构

参照`trajectory.py`模块，输出的轨迹包含以下字段：

### 基础信息
- `dataset_name`: 数据集名称
- `start_time`: 轨迹开始时间（Unix时间戳）
- `end_time`: 轨迹结束时间（Unix时间戳）
- `duration`: 轨迹持续时间（秒）
- `point_count`: 轨迹点数量

### 速度统计
- `avg_speed`: 平均速度
- `max_speed`: 最大速度  
- `min_speed`: 最小速度
- `std_speed`: 速度标准差

### 其他统计
- `avp_ratio`: AVP标志比例（0-1之间）
- `polygon_ids`: 相交的polygon ID列表
- `geometry`: 轨迹线几何（LineString）

## 安装和依赖

模块依赖以下包：
- `geopandas`: 地理数据处理
- `pandas`: 数据处理
- `shapely`: 几何对象处理
- `sqlalchemy`: 数据库连接
- `psycopg2`: PostgreSQL驱动

## 使用方法

### 1. 命令行使用

#### 基本语法
```bash
python -m spdatalab.dataset.polygon_trajectory_query --input <geojson文件> [选项]
```

#### 参数说明

##### 基本参数
- `--input`: 输入GeoJSON文件路径（必需）
- `--table`: 输出数据库表名（可选）
- `--output`: 输出GeoJSON文件路径（可选）

##### 性能优化参数
- `--batch-threshold`: 批量查询vs分块查询的阈值（默认50）
- `--chunk-size`: 分块查询的块大小（默认20）
- `--limit`: 每个polygon的轨迹点限制数量（默认10000）
- `--batch-insert`: 批量插入数据库的批次大小（默认1000）

##### 功能选项
- `--min-points`: 构建轨迹的最小点数（默认2）
- `--no-speed-stats`: 禁用速度统计计算
- `--no-avp-stats`: 禁用AVP统计计算
- `--verbose`: 启用详细日志

**注意**: 必须指定`--table`或`--output`中的至少一个。

#### 配置参数详解

| 参数 | 作用 | 推荐值 | 说明 |
|------|------|--------|------|
| `batch_threshold` | 决定查询策略切换点 | 小规模：20-50<br/>大规模：100-200 | ≤阈值使用批量查询，>阈值使用分块查询 |
| `chunk_size` | 分块查询时每块大小 | 小内存：10-20<br/>大内存：50-100 | 影响内存使用和查询并发度 |
| `limit_per_polygon` | 每polygon轨迹点上限 | 精细分析：50000<br/>快速预览：10000 | 控制数据量和查询时间 |
| `batch_insert_size` | 数据库批量插入大小 | 标准：1000<br/>高性能：2000-5000 | 影响插入速度和内存使用 |

#### 使用示例

```bash
# 1. 基础用法：默认高性能配置
python -m spdatalab.dataset.polygon_trajectory_query \
    --input polygons.geojson \
    --table my_trajectories

# 2. 高性能模式：自定义批量查询参数
python -m spdatalab.dataset.polygon_trajectory_query \
    --input polygons.geojson \
    --table my_trajectories \
    --batch-threshold 30 \
    --chunk-size 15 \
    --batch-insert 500

# 3. 同时保存到数据库和文件
python -m spdatalab.dataset.polygon_trajectory_query \
    --input polygons.geojson \
    --table my_trajectories \
    --output trajectories.geojson

# 4. 优化轨迹质量和统计选项
python -m spdatalab.dataset.polygon_trajectory_query \
    --input polygons.geojson \
    --table my_trajectories \
    --limit 20000 \
    --min-points 3 \
    --no-speed-stats \
    --verbose

# 5. 大规模处理优化
python -m spdatalab.dataset.polygon_trajectory_query \
    --input large_polygons.geojson \
    --table large_trajectories \
    --batch-threshold 100 \
    --chunk-size 50 \
    --batch-insert 2000
```

### 2. Python API使用

#### 基础API使用
```python
from spdatalab.dataset.polygon_trajectory_query import process_polygon_trajectory_query

# 使用默认高性能配置
stats = process_polygon_trajectory_query(
    geojson_file="polygons.geojson",
    output_table="my_trajectories",
    output_geojson="trajectories.geojson"
)

# 检查处理结果
if stats['success']:
    print(f"✅ 成功处理 {stats['polygon_count']} 个polygon")
    print(f"📊 查询策略: {stats['query_stats']['strategy']}")
    print(f"🔍 找到轨迹点: {stats['query_stats']['total_points']:,}")
    print(f"🛤️ 构建轨迹: {stats['build_stats']['valid_trajectories']}")
    print(f"⏱️ 总用时: {stats['total_duration']:.2f}s")
```

#### 高级配置API
```python
from spdatalab.dataset.polygon_trajectory_query import (
    process_polygon_trajectory_query,
    PolygonTrajectoryConfig
)

# 自定义高性能配置
config = PolygonTrajectoryConfig(
    batch_threshold=30,          # 批量查询阈值
    chunk_size=15,               # 分块大小
    limit_per_polygon=20000,     # 每polygon轨迹点限制
    batch_insert_size=1000,      # 批量插入大小
    min_points_per_trajectory=3, # 最小轨迹点数
    enable_speed_stats=True,     # 启用速度统计
    enable_avp_stats=True        # 启用AVP统计
)

stats = process_polygon_trajectory_query(
    geojson_file="polygons.geojson",
    output_table="high_performance_trajectories",
    config=config
)
```

#### 直接API调用（专家模式）
```python
from spdatalab.dataset.polygon_trajectory_query import (
    HighPerformancePolygonTrajectoryQuery,
    PolygonTrajectoryConfig,
    load_polygons_from_geojson
)

# 创建查询器
config = PolygonTrajectoryConfig(batch_threshold=50, chunk_size=20)
query_processor = HighPerformancePolygonTrajectoryQuery(config)

# 分步执行，完全控制
polygons = load_polygons_from_geojson("polygons.geojson")
points_df, query_stats = query_processor.query_intersecting_trajectory_points(polygons)
trajectories, build_stats = query_processor.build_trajectories_from_points(points_df)
saved_count, save_stats = query_processor.save_trajectories_to_table(trajectories, "custom_table")

print(f"查询统计: {query_stats}")
print(f"构建统计: {build_stats}")
print(f"保存统计: {save_stats}")
```

## 输入格式

### GeoJSON文件格式

支持以下GeoJSON格式：

#### 1. FeatureCollection格式（推荐）
```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "properties": {
        "id": "area_1",
        "name": "测试区域1"
      },
      "geometry": {
        "type": "Polygon",
        "coordinates": [[
          [116.3, 39.9],
          [116.4, 39.9],
          [116.4, 40.0],
          [116.3, 40.0],
          [116.3, 39.9]
        ]]
      }
    }
  ]
}
```

#### 2. 单个Polygon格式
```json
{
  "type": "Polygon",
  "coordinates": [[
    [116.3, 39.9],
    [116.4, 39.9],
    [116.4, 40.0],
    [116.3, 40.0],
    [116.3, 39.9]
  ]]
}
```

### 坐标系统
- 使用WGS84坐标系统（EPSG:4326）
- 坐标格式：[经度, 纬度]

## 输出格式

### 1. 数据库表结构

创建的数据库表包含以下列：
```sql
CREATE TABLE table_name (
    id serial PRIMARY KEY,
    dataset_name text NOT NULL,
    start_time bigint,
    end_time bigint,
    duration bigint,
    point_count integer,
    avg_speed numeric(8,2),
    max_speed numeric(8,2),
    min_speed numeric(8,2),
    std_speed numeric(8,2),
    avp_ratio numeric(5,3),
    polygon_ids text[],
    geometry geometry(LINESTRING, 4326),
    created_at timestamp DEFAULT CURRENT_TIMESTAMP
);
```

### 2. GeoJSON文件格式

输出的GeoJSON文件包含轨迹线（LineString）和属性信息：
```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "properties": {
        "dataset_name": "data_001",
        "start_time": 1609459200,
        "end_time": 1609462800,
        "duration": 3600,
        "point_count": 120,
        "avg_speed": 25.5,
        "max_speed": 45.2,
        "min_speed": 0.0,
        "std_speed": 8.3,
        "avp_ratio": 0.85,
        "polygon_ids": "area_1,area_2"
      },
      "geometry": {
        "type": "LineString",
        "coordinates": [
          [116.35, 39.92],
          [116.36, 39.93],
          ...
        ]
      }
    }
  ]
}
```

## 性能优化与调优

### 🚀 查询策略自动优化
- **批量查询**（≤50个polygon）：使用UNION ALL，单次查询所有polygon，速度最快
- **分块查询**（>50个polygon）：分块处理，稳定性更好，避免查询超时
- **智能切换**：根据polygon数量自动选择最优策略

### 💾 数据库写入优化
- **批量插入**：使用`method='multi'`进行批量插入，显著提升写入速度
- **事务保护**：写入过程使用事务，确保数据一致性
- **多重索引**：自动创建空间索引、属性索引，优化后续查询

### 🔧 性能调优建议

#### 小规模处理（≤50个polygon）
```python
config = PolygonTrajectoryConfig(
    batch_threshold=50,          # 使用批量查询
    limit_per_polygon=20000,     # 允许更多轨迹点
    batch_insert_size=2000       # 大批量插入
)
```

#### 中规模处理（50-200个polygon）
```python
config = PolygonTrajectoryConfig(
    batch_threshold=30,          # 提前切换到分块
    chunk_size=20,               # 适中分块大小
    limit_per_polygon=15000,     # 平衡数据量
    batch_insert_size=1000       # 标准插入大小
)
```

#### 大规模处理（>200个polygon）
```python
config = PolygonTrajectoryConfig(
    batch_threshold=100,         # 大阈值减少分块数
    chunk_size=50,               # 大分块提升效率
    limit_per_polygon=10000,     # 控制单polygon数据量
    batch_insert_size=500        # 小批量避免内存压力
)
```

### ⏱️ 性能基准参考

| Polygon数量 | 推荐策略 | 预期时间 | 优化要点 |
|------------|----------|----------|----------|
| 1-20 | 批量查询 | <5秒 | 提高limit_per_polygon |
| 21-50 | 批量查询 | 5-15秒 | 优化batch_insert_size |
| 51-100 | 分块查询 | 15-45秒 | 调整chunk_size |
| 101-500 | 分块查询 | 1-5分钟 | 降低limit_per_polygon |
| >500 | 分块查询 | >5分钟 | 考虑分批处理 |

### 🛠️ 内存与资源优化
- **内存使用**：分批处理避免内存溢出，大规模数据建议减小`chunk_size`
- **并发控制**：PostgreSQL连接池，避免连接过多
- **磁盘空间**：确保足够空间存储轨迹表和索引

### 📊 监控与诊断
使用`--verbose`参数获取详细性能统计：
```bash
python -m spdatalab.dataset.polygon_trajectory_query \
    --input polygons.geojson --table my_trajectories --verbose
```

关键指标监控：
- **查询时间**：单次空间查询耗时
- **构建时间**：轨迹构建和统计计算时间
- **保存时间**：数据库批量写入时间
- **处理速度**：轨迹点/秒的处理速率

## 示例和测试

### 🎯 运行高性能示例
```bash
cd examples
python polygon_trajectory_query_example.py
```

该示例提供多种演示模式：
1. **基础示例** - 展示默认高性能配置
2. **高性能示例** - 展示自定义优化配置
3. **直接API示例** - 展示分步API调用
4. **运行所有示例** - 完整功能演示

### 🧪 性能基准测试
```bash
python performance_benchmark.py
```

基准测试将：
- 测试不同规模的polygon处理性能
- 对比不同配置参数的效果
- 生成详细的性能分析报告
- 提供最佳配置推荐

### 🔧 快速功能测试
```bash
python test_polygon_trajectory_quick.py
```

快速测试包括：
- 模块导入验证
- 高性能查询器创建
- 批量查询功能测试
- 轨迹构建功能测试
- 完整工作流测试

### 📊 测试数据
示例和测试使用北京地区的polygon：
- **基础测试区域**: [116.3, 39.9] - [116.4, 40.0]
- **扩展测试区域**: [116.35, 39.85] - [116.45, 39.95]
- **性能测试**: 动态生成5-100个不同大小的polygon

## 故障排除

### 常见错误

#### 1. 未找到轨迹点
```
WARNING - 未找到任何相交的轨迹点
```
**解决方案**:
- 检查polygon坐标是否正确（经度在前，纬度在后）
- 确认polygon区域内确实有轨迹数据
- 尝试扩大polygon范围

#### 2. 数据库连接失败
```
ERROR - 查询轨迹点失败: connection failed
```
**解决方案**:
- 检查数据库连接配置
- 确认PostgreSQL服务正在运行
- 验证数据库表`public.ddi_data_points`存在

#### 3. GeoJSON格式错误
```
ERROR - 不支持的GeoJSON格式: Point
```
**解决方案**:
- 确保GeoJSON包含Polygon或MultiPolygon几何
- 检查JSON格式是否正确
- 使用在线GeoJSON验证器验证文件

### 调试建议

1. **启用详细日志**: 使用`--verbose`参数查看详细执行信息
2. **检查输入数据**: 使用GIS软件（如QGIS）验证polygon位置
3. **分步测试**: 先用小范围polygon测试，确认功能正常后再扩大范围

## 与其他模块的关系

### 1. 与trajectory.py的关系
- 复用轨迹构建逻辑和字段结构
- 共享数据库配置和连接方式
- 统计信息计算方法保持一致

### 2. 与bbox模块的关系
- 可作为bbox分析的后续步骤
- bbox提供粗粒度区域筛选，polygon提供精细轨迹查询
- 建议工作流：bbox → QGIS筛选 → polygon轨迹查询

### 3. 扩展可能性
- 支持缓冲区查询（polygon周围N米范围）
- 集成轨迹变化点检测功能
- 支持时间范围过滤
- 添加轨迹质量评估指标

## 更新历史

- **v2.0** (2024): 高性能版本
  - ✨ 智能批量查询策略（UNION ALL + 分块查询）
  - ⚡ 高效数据库批量写入优化
  - 📊 详细性能统计和监控
  - 🔧 丰富的配置参数调优
  - 🧪 完整的性能基准测试套件
  - 📚 专业的性能调优指南

- **v1.0** (2024): 初始版本
  - 基础polygon轨迹查询功能
  - 支持GeoJSON输入输出
  - 基本的轨迹统计计算 