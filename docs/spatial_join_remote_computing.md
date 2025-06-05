# 空间连接远端计算功能

## 概述

`SpatialJoin` 模块新增了高效的分批远端计算功能，用于解决FDW（Foreign Data Wrapper）方式在复杂空间计算中的性能问题。

## 背景

原始的FDW方式虽然可以访问远程数据，但在进行复杂的空间叠置分析时存在以下问题：
- 网络传输开销大
- 远程计算资源利用率低
- 大批量数据处理效率低下

## 新的解决方案

### 分批推送+远端计算模式

新的 `batch_spatial_join_with_remote` 方法采用以下策略：

1. **按城市分批获取本地数据**：从 `clips_bbox` 表按 `city_id` 分批获取数据
2. **推送到远端临时表**：将每个城市的数据推送到远端数据库作为临时表
3. **远端执行空间计算**：在远端与 `full_intersection` 进行高效的空间叠置
4. **返回结果并合并**：将各城市结果合并返回本地

### 核心优势

- ✅ **高效计算**：充分利用远端数据库的空间索引和计算资源
- ✅ **内存可控**：按城市分批处理，避免大量数据一次性加载
- ✅ **地理优化**：同城市数据地理位置相近，空间查询更高效
- ✅ **自动清理**：临时表自动创建和清理
- ✅ **灵活配置**：支持多种空间关系和统计选项
- ✅ **并行友好**：可以并行处理不同城市的数据

## 使用方法

### 基本用法

```python
from spdatalab.fusion.spatial_join import SpatialJoin, SpatialRelation

# 创建空间连接器
joiner = SpatialJoin()

# 执行分批空间连接
result = joiner.batch_spatial_join_with_remote(
    left_table="clips_bbox",
    remote_table="full_intersection",
    batch_by_city=True,
    spatial_relation=SpatialRelation.INTERSECTS,
    summarize=True,
    summary_fields={
        "intersection_count": "count",
        "nearest_distance": "distance"
    }
)
```

### 主要参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `left_table` | str | "clips_bbox" | 本地左表名 |
| `remote_table` | str | "full_intersection" | 远端右表名 |
| `batch_by_city` | bool | True | 是否按城市ID分批（推荐） |
| `spatial_relation` | SpatialRelation | INTERSECTS | 空间关系 |
| `distance_meters` | float | None | 距离阈值（用于DWITHIN） |
| `summarize` | bool | True | 是否进行统计汇总 |
| `summary_fields` | dict | None | 统计字段和方法 |
| `limit_batches` | int | None | 限制处理的城市数量（测试用） |
| `city_ids` | list | None | 指定要处理的城市ID列表 |
| `output_table` | str | None | 输出表名 |

### 支持的空间关系

- `INTERSECTS`：相交（默认）
- `WITHIN`：包含于
- `CONTAINS`：包含
- `TOUCHES`：相切
- `CROSSES`：穿越
- `OVERLAPS`：重叠
- `DWITHIN`：距离范围内（需指定distance_meters）

### 统计汇总选项

支持的统计方法：
- `count`：计数
- `distance`：最近距离（米）
- `sum`：求和
- `mean`/`avg`：平均值
- `max`：最大值
- `min`：最小值

### 使用示例

#### 1. 路口相交分析
```python
result = joiner.batch_spatial_join_with_remote(
    batch_by_city=True,
    spatial_relation=SpatialRelation.INTERSECTS,
    summary_fields={
        "intersection_count": "count",
        "nearest_distance": "distance"
    }
)
```

#### 2. 缓冲区分析
```python
result = joiner.batch_spatial_join_with_remote(
    batch_by_city=True,
    spatial_relation=SpatialRelation.DWITHIN,
    distance_meters=50.0,
    summary_fields={
        "nearby_intersections": "count",
        "min_distance": "distance"
    }
)
```

#### 3. 详细信息获取（非汇总）
```python
result = joiner.batch_spatial_join_with_remote(
    batch_by_city=True,
    summarize=False,
    fields_to_add=["intersection_id", "road_type", "traffic_signal"]
)
```

#### 4. 指定特定城市处理
```python
result = joiner.batch_spatial_join_with_remote(
    batch_by_city=True,
    city_ids=["singapore", "boston"],  # 只处理指定城市
    spatial_relation=SpatialRelation.INTERSECTS,
    summary_fields={
        "intersection_count": "count",
        "nearest_distance": "distance"
    }
)
```

## 配置要求

### 数据库连接

确保以下连接配置正确：

```python
# 本地数据库
LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"

# 远端数据库（rcdatalake_gy1）
REMOTE_DSN = "postgresql+psycopg://**:**@10.170.30.193:9001/rcdatalake_gy1"
```

### 数据表要求

- **本地表** (`clips_bbox`)：必须包含 `scene_token`、`city_id` 和 `geometry` 字段
- **远端表** (`full_intersection`)：必须包含 `wkb_geometry` 字段和相关属性字段

## 性能优化建议

1. **按城市分批的优势**
   - 地理位置相近，空间查询效率更高
   - 可以并行处理不同城市
   - 便于按城市进行结果分析

2. **使用空间索引**
   - 确保远端表有适当的空间索引
   - 临时表会自动创建空间索引

3. **测试先行**
   - 使用 `limit_batches` 参数先测试少数几个城市
   - 使用 `city_ids` 参数测试特定城市
   - 确认结果正确后再处理全量数据

4. **监控资源使用**
   - 关注远端数据库的CPU和内存使用
   - 可以通过 `city_ids` 参数控制并发城市数量

## 故障排除

### 常见问题

1. **连接失败**
   - 检查数据库连接字符串
   - 确认网络连通性和权限

2. **空间计算错误**
   - 检查几何数据的坐标系统
   - 确认空间关系参数正确

3. **内存不足**
   - 减小批次大小
   - 检查临时表是否正确清理

### 日志调试

启用详细日志来诊断问题：

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## 迁移指南

### 从FDW方式迁移

原有的 `join_attributes_by_location` 方法仍然可用，但推荐迁移到新方法：

```python
# 旧方式（FDW）
result = joiner.join_attributes_by_location(
    "clips_bbox", "intersections", 
    summarize=True,
    summary_fields={"count": "count"}
)

# 新方式（远端计算）
result = joiner.batch_spatial_join_with_remote(
    summarize=True,
    summary_fields={"intersection_count": "count"}
)
```

### 主要差异

1. **表名**：远端直接使用 `full_intersection`，不需要视图映射
2. **几何列**：远端表使用 `wkb_geometry` 而非 `geometry`
3. **性能**：新方法显著提升大批量数据的处理效率

## TODO 项目

- [ ] 支持更多空间关系
- [ ] 添加并发批次处理
- [ ] 优化临时表的索引策略
- [ ] 集成进度跟踪和断点续传
- [ ] 废弃原有FDW相关方法 