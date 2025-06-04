# 空间连接功能使用指南

## 概述

新的空间连接模块提供了类似QGIS中"join attributes by location"的功能，专门用于bbox与各种要素的空间叠置分析。

## 核心特点

- **简化设计**: 专注于bbox与其他要素的空间连接
- **通用接口**: 支持与路口、道路、POI等各种要素类型的连接
- **多种空间关系**: 支持相交、包含、距离范围等空间关系
- **灵活字段选择**: 可以选择特定字段并进行汇总统计
- **易于扩展**: 后续添加新要素类型只需调整参数

## 依赖要求

- **PostGIS数据库**: 项目使用PostGIS进行空间计算
- **Python依赖**: pandas, geopandas, sqlalchemy, psycopg

## 快速开始

### 1. 最简单的用法

```python
from spdatalab.fusion import SpatialJoin

# 初始化
spatial_join = SpatialJoin()

# bbox与路口相交分析（20米缓冲区）
results = spatial_join.bbox_intersect_features(
    feature_table="intersections",
    buffer_meters=20.0
)

print(f"找到 {len(results)} 个bbox-路口关联")
```

### 2. 自定义字段选择

```python
# 选择特定字段并重命名
results = spatial_join.join_attributes_by_location(
    left_table="clips_bbox",
    right_table="intersections",
    spatial_relation="dwithin",
    distance_meters=50.0,
    select_fields={
        "intersection_id": "inter_id",           # 重命名
        "intersection_type": "inter_type",       # 重命名
        "intersection_count": "count",           # 统计数量
        "min_distance": "distance_meters|min"   # 最小距离
    }
)
```

### 3. 与不同要素类型的分析

```python
# 与路口的分析
intersection_results = spatial_join.bbox_intersect_features(
    feature_table="intersections",
    buffer_meters=25.0
)

# 与道路的分析（假设有roads表）
road_results = spatial_join.bbox_intersect_features(
    feature_table="roads", 
    feature_type="roads",
    buffer_meters=10.0
)

# 与POI的分析（假设有pois表）
poi_results = spatial_join.bbox_intersect_features(
    feature_table="pois",
    feature_type="pois", 
    buffer_meters=100.0
)
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

# 使用缓冲区
spdatalab spatial-join \
    --right-table intersections \
    --buffer-meters 30 \
    --output-table bbox_intersection_results
```

### 自定义字段选择和导出

```bash
# 选择特定字段并导出CSV
spdatalab spatial-join \
    --right-table intersections \
    --select-fields "inter_id,inter_type,count:count" \
    --output-file results.csv

# 同时保存到数据库和文件
spdatalab spatial-join \
    --right-table intersections \
    --buffer-meters 25 \
    --output-table analysis_results \
    --output-file analysis_results.csv
```

## 支持的空间关系

- **intersects**: 相交（默认）
- **within**: 左表要素完全在右表要素内
- **contains**: 左表要素完全包含右表要素
- **touches**: 边界相接触
- **crosses**: 相交但不重叠
- **overlaps**: 部分重叠
- **dwithin**: 指定距离范围内

## 字段选择和汇总

### 简单字段选择
```python
select_fields = {
    "new_name": "original_field"  # 重命名字段
}
```

### 汇总统计
```python
select_fields = {
    "intersection_count": "count",                    # 计数
    "avg_value": "some_field|mean",                   # 平均值
    "min_distance": "distance_meters|min",           # 最小距离
    "max_value": "some_field|max"                     # 最大值
}
```

## 实际应用场景

### 1. 交通路口分析
```python
# 分析每个bbox附近的路口情况
intersection_analysis = spatial_join.bbox_intersect_features(
    feature_table="intersections",
    buffer_meters=50.0,
    summary_fields={
        "nearby_intersections": "count",
        "nearest_intersection_distance": "min_distance"
    }
)

# 查看结果统计
print(f"平均每个bbox附近有 {intersection_analysis['nearby_intersections'].mean():.1f} 个路口")
```

### 2. 道路网络分析
```python
# 分析bbox与道路网络的关系
road_analysis = spatial_join.join_attributes_by_location(
    left_table="clips_bbox",
    right_table="roads",
    spatial_relation="intersects",
    select_fields={
        "road_type": "road_type",
        "road_name": "road_name",
        "road_count": "count"
    }
)

# 按道路类型统计
road_type_stats = road_analysis['road_type'].value_counts()
print("道路类型分布:")
print(road_type_stats)
```

### 3. 兴趣点密度分析
```python
# 分析bbox周围的POI密度
poi_analysis = spatial_join.bbox_intersect_features(
    feature_table="pois",
    feature_type="pois",
    buffer_meters=200.0,
    summary_fields={
        "poi_density": "count",
        "nearest_poi_distance": "min_distance"
    }
)

# 统计POI密度分布
high_density_areas = poi_analysis[poi_analysis['poi_density'] > 10]
print(f"高密度POI区域: {len(high_density_areas)} 个bbox")
```

## 性能优化建议

1. **建立空间索引** (必需)
```sql
CREATE INDEX IF NOT EXISTS idx_clips_bbox_geometry 
    ON clips_bbox USING GIST(geometry);

CREATE INDEX IF NOT EXISTS idx_intersections_geom 
    ON intersections USING GIST(geom);
```

2. **合理选择缓冲区大小**
   - 路口分析: 20-50米
   - 道路分析: 5-15米  
   - POI分析: 100-500米

3. **使用合适的空间关系**
   - 精确分析使用`intersects`
   - 邻近分析使用`dwithin`
   - 包含关系使用`within`/`contains`

4. **分批处理大数据集**
```python
# 按城市分批处理
cities = ['beijing', 'shanghai', 'guangzhou']
all_results = []

for city_id in cities:
    results = spatial_join.join_attributes_by_location(
        left_table="clips_bbox",
        right_table="intersections",
        where_clause=f"l.city_id = '{city_id}'"
    )
    all_results.append(results)
    print(f"{city_id}: {len(results)} 条记录")

# 合并结果
import pandas as pd
final_results = pd.concat(all_results, ignore_index=True)
```

## 扩展到新要素类型

添加新的要素类型分析非常简单，只需要知道表名和字段名：

```python
# 假设有新的要素表 "traffic_lights"
traffic_light_analysis = spatial_join.bbox_intersect_features(
    feature_table="traffic_lights",
    feature_type="traffic_lights",
    buffer_meters=30.0,
    summary_fields={
        "traffic_light_count": "count",
        "nearest_traffic_light_distance": "min_distance"
    }
)

# 或者使用完整接口进行更复杂的分析
custom_analysis = spatial_join.join_attributes_by_location(
    left_table="clips_bbox", 
    right_table="traffic_lights",
    spatial_relation="dwithin",
    distance_meters=100.0,
    select_fields={
        "light_type": "light_type",
        "light_status": "status", 
        "light_count": "count"
    },
    where_clause="r.status = 'active'"  # 只考虑活跃的交通灯
)
```

## 常见问题解答

### Q: 如何处理没有匹配结果的情况？
A: 使用LEFT JOIN（默认），bbox记录会被保留，关联字段为NULL。可以用`dropna()`删除无匹配的记录。

### Q: 如何获得更精确的距离计算？
A: 系统会自动将几何转换到Web Mercator投影（EPSG:3857）进行距离计算，精度约为米级。

### Q: 能否同时与多个要素表进行连接？
A: 目前一次只能连接两个表，但可以多次调用进行链式连接：
```python
# 先与路口连接
result1 = spatial_join.join_attributes_by_location("clips_bbox", "intersections")
# 保存中间结果到新表，然后与道路连接
result2 = spatial_join.join_attributes_by_location("result1_table", "roads")
```

### Q: 如何处理复杂的空间关系？
A: 可以在`where_clause`中添加额外的PostGIS空间函数条件：
```python
results = spatial_join.join_attributes_by_location(
    left_table="clips_bbox",
    right_table="complex_features", 
    where_clause="ST_Area(r.geom) > 1000 AND r.category = 'important'"
)
```

### Q: 命令行运行时提示找不到命令？
A: 确保已安装项目并且环境变量正确：
```bash
cd src && python -m spdatalab.cli spatial-join --help
# 或者
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"
spdatalab spatial-join --help
```

## 示例数据结果

典型的分析结果包含以下字段：

```
scene_token | data_name | city_id | geometry | intersection_id | intersection_type | intersection_count | min_distance
------------|-----------|---------|----------|-----------------|-------------------|--------------------|--------------
scene_001   | data_1    | beijing | POLYGON  | inter_001       | signal            | 3                  | 15.5
scene_002   | data_1    | beijing | POLYGON  | inter_002       | stop_sign         | 1                  | 25.2
...
```

## 总结

这个简化的空间连接功能让你可以轻松地将bbox与任何空间要素进行关联分析：

1. **简单易用**: 一行代码即可完成基础分析
2. **功能强大**: 支持多种空间关系和字段汇总
3. **性能优化**: 基于PostGIS的高效空间计算
4. **易于扩展**: 添加新要素类型无需修改代码

这为后续的深入空间分析提供了坚实的基础。 