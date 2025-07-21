# Polygon道路分析指南

## 概述

`polygon_road_analysis`模块提供基于polygon区域的道路元素分析功能。与轨迹分析不同，这个模块专注于：

1. 基于polygon区域批量查找roads/intersections/lanes  
2. 高效的批量空间查询，避免逐个polygon处理
3. 使用临时表和空间索引优化性能

## 主要功能

- 从GeoJSON文件加载多个polygon
- 批量查询polygon内的roads/intersections
- 从roads获取对应的lanes
- 按polygon组织查询结果并保存到数据库

## 使用方法

### 1. 准备输入文件

创建包含polygon的GeoJSON文件，格式如下：

```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "properties": {
        "polygon_id": "area_001",
        "name": "商业区A",
        "category": "commercial"
      },
      "geometry": {
        "type": "Polygon",
        "coordinates": [[
          [116.3973, 39.9093],
          [116.4073, 39.9093],
          [116.4073, 39.9193],
          [116.3973, 39.9193],
          [116.3973, 39.9093]
        ]]
      }
    }
  ]
}
```

**要求：**
- 必须是`FeatureCollection`类型
- 每个Feature的geometry必须是`Polygon`类型
- properties中可包含`polygon_id`字段（用作唯一标识）
- 如果没有`polygon_id`，会自动生成

### 2. 命令行使用

```bash
# 基本用法
python -m spdatalab.fusion.polygon_road_analysis \
    --input-geojson areas.geojson

# 指定批量分析ID和配置
python -m spdatalab.fusion.polygon_road_analysis \
    --input-geojson areas.geojson \
    --batch-analysis-id "commercial_areas_20241201" \
    --polygon-batch-size 20 \
    --enable-parallel-queries

# 输出详细统计
python -m spdatalab.fusion.polygon_road_analysis \
    --input-geojson areas.geojson \
    --output-summary \
    --verbose
```

### 3. Python API使用

```python
from spdatalab.fusion.polygon_road_analysis import (
    analyze_polygons_from_geojson,
    PolygonRoadAnalysisConfig
)

# 创建配置
config = PolygonRoadAnalysisConfig(
    polygon_batch_size=30,
    enable_parallel_queries=True,
    max_polygon_area=5000000  # 5平方公里
)

# 执行分析
analysis_id, summary = analyze_polygons_from_geojson(
    geojson_file="areas.geojson",
    config=config,
    batch_analysis_id="my_analysis_20241201"
)

# 查看结果
print(f"分析ID: {analysis_id}")
print(f"处理的polygon数: {summary['total_polygons']}")
print(f"找到的road数: {summary['total_roads']}")
print(f"找到的intersection数: {summary['total_intersections']}")
print(f"找到的lane数: {summary['total_lanes']}")
```

### 4. 高级用法：直接使用分析器

```python
from spdatalab.fusion.polygon_road_analysis import BatchPolygonRoadAnalyzer

# 创建分析器
analyzer = BatchPolygonRoadAnalyzer(config)

# 执行分析
analysis_id = analyzer.analyze_polygons_from_geojson(
    "areas.geojson", 
    "custom_batch_id"
)

# 获取详细摘要
summary = analyzer.get_analysis_summary(analysis_id)
```

## 输出结果

分析结果保存在以下数据库表中：

### 1. 主分析表 (`polygon_road_analysis`)
记录每次分析的总体统计信息：
- `analysis_id`: 分析ID
- `batch_analysis_id`: 批量分析ID  
- `total_polygons`: 处理的polygon数量
- `total_roads`: 找到的road总数
- `total_intersections`: 找到的intersection总数
- `total_lanes`: 找到的lane总数
- `processing_time_seconds`: 处理时间（秒）

### 2. Roads结果表 (`polygon_roads`)
每个polygon内找到的roads详情：
- `polygon_id`: polygon标识
- `road_id`: road ID
- `road_type`: road类型
- `intersection_type`: 相交类型（WITHIN/INTERSECTS）
- `intersection_ratio`: 相交长度比例
- `geometry`: road几何信息

### 3. Intersections结果表 (`polygon_intersections`)
每个polygon内找到的intersections详情：
- `polygon_id`: polygon标识
- `intersection_id`: intersection ID
- `intersection_type`: intersection类型
- `geometry`: intersection几何信息

### 4. Lanes结果表 (`polygon_lanes`)
从roads获取的lanes详情：
- `polygon_id`: polygon标识（通过road_id关联）
- `lane_id`: lane ID
- `road_id`: 所属road ID
- `lane_type`: lane类型
- `geometry`: lane几何信息

## 配置选项

### PolygonRoadAnalysisConfig

```python
@dataclass
class PolygonRoadAnalysisConfig:
    # 数据库配置
    local_dsn: str = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
    remote_catalog: str = "rcdatalake_gy1"
    
    # 远程表名配置
    lane_table: str = "full_lane"
    intersection_table: str = "full_intersection"
    road_table: str = "full_road"
    
    # 批量处理配置
    polygon_batch_size: int = 50  # 每批处理的polygon数量
    enable_parallel_queries: bool = True  # 启用并行查询
    
    # 查询限制配置
    max_polygon_area: float = 10000000  # 最大polygon面积（平方米）
    max_roads_per_polygon: int = 1000  # 单个polygon最大road数量
    max_intersections_per_polygon: int = 200  # 单个polygon最大intersection数量
    
    # 边界处理配置
    include_boundary_roads: bool = True  # 是否包含边界roads
    boundary_inclusion_threshold: float = 0.1  # 边界road包含阈值
```

## 性能优化

### 1. 批量处理
- 默认每批处理50个polygon，可通过`polygon_batch_size`调整
- 大量polygon会分批处理，避免内存溢出

### 2. 并行查询
- 默认启用并行查询roads和intersections
- 可通过`enable_parallel_queries`禁用

### 3. 空间索引
- 充分利用PostGIS和Hive的空间索引
- 使用边界框预过滤提高查询效率

### 4. 查询限制
- 设置各种限制参数防止查询过载
- 超大polygon会产生警告

## 错误处理

常见问题及解决方法：

### 1. GeoJSON格式错误
```
ValueError: GeoJSON必须是FeatureCollection类型
```
**解决方法：** 检查GeoJSON文件格式，确保type为"FeatureCollection"

### 2. Polygon无效
```
警告: 无效的polygon: polygon_001
```
**解决方法：** 检查polygon几何是否有效，坐标是否正确闭合

### 3. 数据库连接失败
```
错误: 批量查询roads失败
```
**解决方法：** 检查数据库连接配置，确保Hive和PostgreSQL都能正常访问

### 4. 内存不足
```
错误: 查询结果过大导致内存溢出
```
**解决方法：** 减少`polygon_batch_size`，或限制polygon面积

## 示例

参见项目中的示例文件：
- `examples/test_polygon_areas.geojson` - 示例polygon数据
- `test_polygon_road_analysis.py` - 测试脚本

## 与其他模块的区别

| 功能 | Polygon分析 | 轨迹分析 |
|------|-------------|----------|
| 输入 | Polygon区域 | 轨迹线 |
| 查询方式 | 区域相交 | 缓冲区查询 |
| 批量处理 | 高效批量查询 | 逐轨迹处理 |
| 主要用途 | 区域道路规划 | 轨迹行为分析 |
| 输出重点 | 道路基础设施 | 轨迹相关性 | 