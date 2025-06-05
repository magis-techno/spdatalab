# 空间连接功能使用指南

## 概述

新的空间连接模块提供了类似QGIS中"join attributes by location"的功能，专门用于bbox与各种要素的空间叠置分析。

## 核心特点

- **QGIS风格设计**: 参数和用法完全模拟QGIS的"join attributes by location"功能
- **简化设计**: 专注于bbox与其他要素的空间连接
- **通用接口**: 支持与路口、道路、POI等各种要素类型的连接
- **多种空间关系**: 支持相交、包含、距离范围等空间关系
- **灵活字段选择**: 类似QGIS的"fields to add"选项，可以选择特定字段或全部字段
- **统计汇总功能**: 支持对连接结果进行count、sum、mean等统计
- **易于扩展**: 后续添加新要素类型只需调整参数

## 依赖要求

- **PostGIS数据库**: 项目使用PostGIS进行空间计算
- **Python依赖**: pandas, geopandas, sqlalchemy, psycopg

## 快速开始

### 1. 最简单的用法 - 连接所有字段

```python
from spdatalab.fusion import SpatialJoin

# 初始化
spatial_join = SpatialJoin()

# 简单连接：添加所有右表字段（类似QGIS中fields to add留空）
results = spatial_join.join_attributes_by_location(
    left_table="clips_bbox",
    right_table="intersections"
)

print(f"连接结果: {len(results)} 条记录，{len(results.columns)} 个字段")
```

### 2. 选择特定字段 - 类似QGIS的"fields to add"

```python
# 只添加指定字段（类似QGIS中在fields to add选择特定字段）
results = spatial_join.join_attributes_by_location(
    left_table="clips_bbox",
    right_table="intersections",
    fields_to_add=["intersection_id", "intersection_type", "road_width"]
)

print("添加的字段:", results.columns.tolist())
```

### 3. 统计汇总模式 - 连接后统计

```python
# 统计汇总（当一个bbox匹配多个要素时）
results = spatial_join.join_attributes_by_location(
    left_table="clips_bbox",
    right_table="intersections",
    spatial_relation="dwithin",
    distance_meters=50,
    summarize=True,  # 开启统计模式
    summary_fields={
        "intersection_count": "count",        # 计算数量
        "nearest_distance": "distance",       # 最近距离
        "avg_road_width": "mean"              # 平均值（需要有road_width字段）
    }
)
```

### 4. 控制连接类型 - 是否保留未匹配的记录

```python
# 保留所有左表记录（LEFT JOIN，默认）
all_results = spatial_join.join_attributes_by_location(
    left_table="clips_bbox",
    right_table="intersections",
    discard_nonmatching=False  # 类似QGIS中不勾选"discard records which could not be joined"
)

# 只保留有匹配的记录（INNER JOIN）  
matched_only = spatial_join.join_attributes_by_location(
    left_table="clips_bbox", 
    right_table="intersections",
    discard_nonmatching=True   # 类似QGIS中勾选"discard records which could not be joined"
)

print(f"全部记录: {len(all_results)}, 仅匹配记录: {len(matched_only)}")
```

## 支持的空间关系

- **intersects**: 相交（默认）
- **within**: 左表要素完全在右表要素内
- **contains**: 左表要素完全包含右表要素
- **touches**: 边界相接触
- **crosses**: 相交但不重叠
- **overlaps**: 部分重叠
- **dwithin**: 指定距离范围内

## 字段选择详解

### 1. 添加所有字段（fields_to_add=None）
```python
# 类似QGIS中fields to add留空
results = spatial_join.join_attributes_by_location(
    left_table="clips_bbox",
    right_table="intersections",
    fields_to_add=None  # 默认值，添加所有右表字段
)
```

### 2. 选择特定字段
```python
# 类似QGIS中在fields to add选择特定字段
results = spatial_join.join_attributes_by_location(
    left_table="clips_bbox",
    right_table="intersections", 
    fields_to_add=["intersection_id", "road_type", "signal_type"]
)
```

### 3. 统计汇总字段
```python
# 当需要对连接结果进行统计时
summary_fields = {
    "feature_count": "count",           # 要素数量
    "nearest_distance": "distance",     # 最近距离
    "total_value": "sum",               # 总和（需要指定字段，如"road_width_sum": "sum"）
    "average_value": "mean",            # 平均值
    "max_value": "max",                 # 最大值
    "min_value": "min",                 # 最小值
    "first_match": "first"              # 第一个匹配记录
}
```

## 命令行使用

### 基础命令

```bash
# 基础相交分析
spdatalab spatial-join --right-table intersections

# 距离范围内连接
spdatalab spatial-join \
    --right-table intersections \
    --spatial-relation dwithin \
    --distance-meters 50

# 使用缓冲区（简化接口）
spdatalab spatial-join \
    --right-table intersections \
    --buffer-meters 30 \
    --output-table bbox_intersection_results
```

## 实际应用场景

### 1. 交通路口分析

```python
# 场景1: 简单连接 - 获取每个bbox内的路口信息
intersection_details = spatial_join.join_attributes_by_location(
    left_table="clips_bbox",
    right_table="intersections",
    fields_to_add=["intersection_id", "intersection_type", "signal_type"]
)

# 场景2: 统计分析 - 计算每个bbox附近的路口数量和距离
intersection_stats = spatial_join.join_attributes_by_location(
    left_table="clips_bbox",
    right_table="intersections", 
    spatial_relation="dwithin",
    distance_meters=50,
    summarize=True,
    summary_fields={
        "intersection_count": "count",
        "nearest_intersection_distance": "distance"
    }
)

print(f"平均每个bbox附近有 {intersection_stats['intersection_count'].mean():.1f} 个路口")
```

### 2. 道路网络分析

```python
# 获取与bbox相交的道路详细信息
road_details = spatial_join.join_attributes_by_location(
    left_table="clips_bbox",
    right_table="roads",
    fields_to_add=["road_name", "road_type", "road_class", "speed_limit"],
    discard_nonmatching=True  # 只保留与道路相交的bbox
)

# 统计每个bbox相交的道路类型
road_stats = spatial_join.join_attributes_by_location(
    left_table="clips_bbox", 
    right_table="roads",
    summarize=True,
    summary_fields={
        "road_count": "count",
        "road_length_total": "sum"  # 假设roads表有length字段
    }
)
```

### 3. 兴趣点密度分析

```python
# POI详细信息连接
poi_details = spatial_join.join_attributes_by_location(
    left_table="clips_bbox",
    right_table="pois",
    spatial_relation="dwithin", 
    distance_meters=200,
    fields_to_add=["poi_name", "poi_category", "rating"]
)

# POI密度统计
poi_density = spatial_join.join_attributes_by_location(
    left_table="clips_bbox",
    right_table="pois",
    spatial_relation="dwithin",
    distance_meters=200, 
    summarize=True,
    summary_fields={
        "poi_count": "count",
        "nearest_poi_distance": "distance",
        "avg_rating": "mean"  # 假设pois表有rating字段
    }
)

# 找出高密度POI区域
high_density = poi_density[poi_density['poi_count'] > 10]
print(f"高密度POI区域: {len(high_density)} 个bbox")
```

## 高级用法示例

### 1. 多步骤分析工作流

```python
# 步骤1: 获取基础连接结果
base_results = spatial_join.join_attributes_by_location(
    left_table="clips_bbox",
    right_table="intersections",
    fields_to_add=["intersection_id", "intersection_type"]
)

# 步骤2: 添加距离统计
distance_stats = spatial_join.join_attributes_by_location(
    left_table="clips_bbox", 
    right_table="intersections",
    spatial_relation="dwithin",
    distance_meters=100,
    summarize=True,
    summary_fields={
        "nearby_count": "count",
        "min_distance": "distance"
    }
)

# 步骤3: 合并结果
import pandas as pd
final_results = pd.merge(
    base_results, 
    distance_stats,
    on=['scene_token'], 
    how='left'
)
```

### 2. 条件过滤分析

```python
# 只分析特定类型的路口
signal_intersections = spatial_join.join_attributes_by_location(
    left_table="clips_bbox",
    right_table="intersections", 
    fields_to_add=["intersection_id", "signal_type"],
    where_clause="r.intersection_type = 'signalized'"
)

# 只分析繁忙道路
major_roads = spatial_join.join_attributes_by_location(
    left_table="clips_bbox",
    right_table="roads",
    fields_to_add=["road_name", "traffic_volume"],
    where_clause="r.road_class IN ('highway', 'arterial')"
)
```

### 3. 批量处理不同城市

```python
cities = ['beijing', 'shanghai', 'guangzhou']
all_results = {}

for city in cities:
    city_results = spatial_join.join_attributes_by_location(
        left_table="clips_bbox",
        right_table="intersections",
        summarize=True,
        summary_fields={
            "intersection_count": "count",
            "nearest_distance": "distance"
        },
        where_clause=f"l.city_id = '{city}'"
    )
    all_results[city] = city_results
    print(f"{city}: {len(city_results)} 个bbox")

# 比较城市间的路口密度
for city, results in all_results.items():
    avg_count = results['intersection_count'].mean()
    print(f"{city}平均路口密度: {avg_count:.1f}")
```

## 性能优化建议

1. **建立空间索引** (必需)
```sql
CREATE INDEX IF NOT EXISTS idx_clips_bbox_geometry 
    ON clips_bbox USING GIST(geometry);

CREATE INDEX IF NOT EXISTS idx_intersections_geometry 
    ON intersections USING GIST(geometry);
```

2. **合理选择连接模式**
   - 简单连接: 当需要详细信息时
   - 统计模式: 当只需要汇总数据时
   - 丢弃未匹配: 当只关心有关联的记录时

3. **合理使用字段选择**
   - 只选择需要的字段可以提高性能
   - 避免传输不必要的大字段

4. **距离参数建议**
   - 路口分析: 20-50米
   - 道路分析: 5-15米  
   - POI分析: 100-500米

## 与QGIS对比

| QGIS功能 | 本工具对应参数 | 说明 |
|---------|---------------|------|
| Join layer | right_table | 要连接的图层 |
| Geometric predicate | spatial_relation | 空间关系选择 |
| Fields to add | fields_to_add | 要添加的字段列表 |
| Discard records which could not be joined | discard_nonmatching | 是否丢弃未匹配记录 |
| Join type | summarize + summary_fields | 统计汇总功能 |

## 常见问题解答

### Q: 如何处理一对多的连接结果？
A: 使用 `summarize=True` 开启统计模式，系统会自动按左表记录分组并汇总右表结果。

### Q: 如何只保留有匹配的记录？
A: 设置 `discard_nonmatching=True`，相当于INNER JOIN。

### Q: 距离计算的精度如何？
A: 使用PostgreSQL的geography类型，在WGS84坐标系上直接计算，考虑地球曲率，精度为米级。

### Q: 如何处理复杂的字段名？
A: 使用 `fields_to_add` 指定确切的字段名，支持包含特殊字符的字段名。

### Q: 能否同时统计多个字段？
A: 可以，在 `summary_fields` 中定义多个统计字段和方法。

## 总结

更新后的空间连接功能完全模拟了QGIS的"join attributes by location"操作：

1. **直观易用**: 参数命名和功能完全对应QGIS
2. **灵活强大**: 支持字段选择、统计汇总、连接类型控制
3. **性能优化**: 基于PostGIS的高效空间计算
4. **易于扩展**: 标准化的接口设计

无论是简单的属性连接还是复杂的空间统计分析，都可以用简洁的代码完成。 