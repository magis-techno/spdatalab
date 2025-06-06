# 空间连接指南

SPDataLab提供强大的空间连接功能，支持bbox与各种要素的空间叠置分析。

## 核心特点

- **QGIS风格设计**：参数和用法模拟QGIS的"join attributes by location"功能
- **生产级性能**：智能策略选择，自动优化查询方式
- **多种空间关系**：支持相交、包含、距离范围等空间关系
- **远端计算**：支持高效的分批远端计算
- **灵活配置**：可选择字段、统计汇总等功能

## 快速开始

### 基本用法

```python
from spdatalab.fusion import quick_spatial_join, ProductionSpatialJoin

# 方式1：快速接口（推荐）
result, stats = quick_spatial_join(num_bbox=100)

# 方式2：完整配置
from spdatalab.fusion import SpatialJoinConfig
config = SpatialJoinConfig(batch_threshold=150)
spatial_join = ProductionSpatialJoin(config)
result, stats = spatial_join.polygon_intersect(num_bbox=100)
```

### 命令行使用

```bash
# 基础相交分析
spdatalab spatial-join --right-table intersections

# 距离范围内连接
spdatalab spatial-join \
    --right-table intersections \
    --spatial-relation dwithin \
    --distance-meters 50

# 使用缓冲区
spdatalab spatial-join \
    --right-table intersections \
    --buffer-meters 30 \
    --output-table bbox_intersection_results
```

## 性能特性

### 智能策略选择

系统根据数据规模自动选择最优策略：

| 规模 | 策略 | 性能 |
|------|------|------|
| ≤200个bbox | 批量查询(UNION ALL) | 最快，<2秒 |
| >200个bbox | 分块查询 | 高扩展性，447 bbox/秒 |

### 自定义配置

支持根据数据规模和需求进行配置：

```python
# 自定义配置
config = SpatialJoinConfig(
    batch_threshold=150,    # 策略切换阈值
    chunk_size=50,         # 分块大小
    max_timeout_seconds=300 # 超时设置
)

spatial_join = ProductionSpatialJoin(config)
result, stats = spatial_join.polygon_intersect(
    num_bbox=500,
    city_filter="boston-seaport"  # 城市过滤
)
```

## 配置参数

### 核心配置

- `batch_threshold`：批量查询vs分块查询的切换阈值（默认200）
- `chunk_size`：分块查询的块大小（默认50）  
- `max_timeout_seconds`：查询超时时间（默认300秒）

### 查询参数

- `num_bbox`：要处理的bbox数量
- `city_filter`：城市过滤条件（可选）
- `chunk_size`：自定义分块大小（可选）

### 返回结果

每次查询返回元组 `(结果DataFrame, 性能统计字典)`：

```python
result, stats = quick_spatial_join(100)

# 性能统计包含：
print(f"使用策略: {stats['strategy']}")      # batch_query 或 chunked_query
print(f"处理速度: {stats['speed_bbox_per_sec']:.1f} bbox/秒")
print(f"总耗时: {stats['total_time']:.2f}秒")
print(f"结果数量: {stats['result_count']}")
```

## 实际应用场景

### 基础polygon相交分析

```python
# 分析100个bbox与路口的相交情况
result, stats = quick_spatial_join(num_bbox=100)

print(f"处理了 {stats['bbox_count']} 个bbox")
print(f"找到 {stats['result_count']} 个相交结果")
print(f"处理速度: {stats['speed_bbox_per_sec']:.1f} bbox/秒")

# 查看结果数据
print(result.head())
```

### 城市级别分析

```python
# 只分析特定城市的数据
boston_result, stats = quick_spatial_join(
    num_bbox=500, 
    city_filter="boston-seaport"
)

print(f"波士顿地区相交统计: {len(boston_result)} 条记录")
```

### 大规模数据处理

```python
# 处理大规模数据（自动使用分块策略）
config = SpatialJoinConfig(chunk_size=30)  # 较小的分块以节省内存
spatial_join = ProductionSpatialJoin(config)

result, stats = spatial_join.polygon_intersect(num_bbox=2000)
print(f"策略: {stats['strategy']}")  # 应该是 'chunked_query'
print(f"总耗时: {stats['total_time']:.2f}秒")
```

## 配置要求

### 数据库连接

```python
# 本地数据库
LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"

# 远端数据库（用于远端计算）
REMOTE_DSN = "postgresql+psycopg://user:pass@remote_host:port/database"
```

### 数据表要求

- **本地表**：必须包含 `scene_token`、`city_id` 和 `geometry` 字段
- **远端表**：必须包含几何字段和相关属性字段

## 性能优化建议

1. **使用空间索引**：确保表有适当的空间索引
2. **按城市分批**：利用地理位置相近的优势
3. **测试先行**：使用小批量数据测试配置
4. **监控资源**：关注数据库CPU和内存使用

## 故障排除

### 常见问题

- **连接失败**：检查数据库连接字符串和网络连通性
- **空间计算错误**：检查几何数据的坐标系统
- **内存不足**：减小批次大小，检查临时表清理

### 调试方法

```python
# 启用详细日志
import logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')

# 小规模测试
result, stats = quick_spatial_join(num_bbox=10)

# 查看详细统计信息
print(f"数据获取时间: {stats['fetch_time']:.2f}秒")
print(f"查询执行时间: {stats['query_time']:.2f}秒")
print(f"使用的策略: {stats['strategy']}")
``` 