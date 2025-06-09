# 空间连接分析使用指南

## 概述

新的空间连接模块支持两种工作模式，解决了原来只能获取相交个数、无法进一步统计分析的问题：

1. **预存模式**（推荐）：将相交关系缓存到数据库，支持快速多维度分析
2. **实时模式**：实时计算相交关系，适合临时分析

## 快速开始

### 1. 基础导入

```python
from src.spdatalab.fusion.spatial_join_production import (
    ProductionSpatialJoin,
    SpatialJoinConfig,
    build_cache,
    analyze_cached_intersections
)
```

### 2. 构建相交关系缓存

```python
# 构建100个bbox的相交关系缓存
cached_count, stats = build_cache(
    num_bbox=100,
    city_filter="boston",  # 可选：指定城市
    force_rebuild=False    # 如果已有缓存则跳过
)

print(f"缓存了 {cached_count} 条相交关系")
```

### 3. 进行统计分析

```python
# 总体统计
overall = analyze_cached_intersections(city_filter="boston")

# 按路口类型分组统计
by_type = analyze_cached_intersections(
    city_filter="boston",
    group_by=["intersectiontype"]
)

# 按场景分组统计
by_scene = analyze_cached_intersections(
    city_filter="boston", 
    group_by=["scene_token"]
)

# 多维度分组
multi_dim = analyze_cached_intersections(
    city_filter="boston",
    group_by=["scene_token", "intersection_type"]
)
```

## 详细功能

### 配置选项

```python
config = SpatialJoinConfig(
    local_dsn="postgresql+psycopg://user:pass@localhost:5432/db",
    remote_dsn="postgresql+psycopg://user:pass@remote:5432/db", 
    batch_threshold=200,      # 批量vs分块查询阈值
    chunk_size=50,           # 分块大小
    intersection_table="bbox_intersection_cache",  # 缓存表名
    enable_cache_table=True  # 启用缓存功能
)

spatial_join = ProductionSpatialJoin(config)
```

### 高级查询

```python
# 初始化
spatial_join = ProductionSpatialJoin()

# 1. 特定路口类型分析
result = spatial_join.analyze_intersections(
    city_filter="boston",
    intersection_types=[1, 2, 3],  # 路口类型是整数值
    group_by=["intersectiontype"]
)

# 2. 特定场景分析
result = spatial_join.analyze_intersections(
    scene_tokens=["scene_001", "scene_002"],  # 只分析这些场景
    group_by=["scene_token", "intersection_type"]
)

# 3. 获取详细信息
details = spatial_join.get_intersection_details(
    city_filter="boston",
    intersection_types=["4-way"],
    limit=100
)
```

### 缓存管理

```python
# 强制重建缓存
cached_count, _ = spatial_join.build_intersection_cache(
    num_bbox=1000,
    city_filter="boston",
    force_rebuild=True  # 强制重建
)

# 清理缓存（内部方法）
spatial_join._clear_cache(city_filter="boston")
```

## 典型分析场景

### 1. 路口热度分析

找出被最多场景相交的热点路口：

```python
# 按路口ID分组，统计相交次数
hotspots = spatial_join.analyze_intersections(
    city_filter="boston",
    group_by=["intersection_id", "intersection_type"]
)

# 找出前10个热点
top_hotspots = hotspots.nlargest(10, 'intersection_count')
print("十大热点路口:")
print(top_hotspots)
```

### 2. 场景复杂度分析

分析每个场景包含的路口数量和类型多样性：

```python
# 按场景统计路口数量
complexity = spatial_join.analyze_intersections(
    city_filter="boston",
    group_by=["scene_token"]
)

# 找出最复杂的场景
complex_scenes = complexity.nlargest(5, 'intersection_count')
print("最复杂的5个场景:")
print(complex_scenes)
```

### 3. 路口类型分布分析

```python
# 统计各类型路口的分布
type_distribution = spatial_join.analyze_intersections(
    city_filter="boston",
    group_by=["intersection_type"]
)

print("路口类型分布:")
for _, row in type_distribution.iterrows():
    print(f"{row['intersection_type']}: {row['intersection_count']}个相交")
```

### 4. 组合分析

```python
# 场景-路口类型组合分析
combo_analysis = spatial_join.analyze_intersections(
    city_filter="boston",
    group_by=["scene_token", "intersection_type"]
)

# 找出每个场景中最常见的路口类型
scene_dominant_types = combo_analysis.loc[
    combo_analysis.groupby('scene_token')['intersection_count'].idxmax()
]
```

## 性能优势

### 缓存 vs 实时查询

| 模式 | 适用场景 | 性能 | 灵活性 |
|------|----------|------|--------|
| 预存缓存 | 重复分析、多维度分析 | 极快（毫秒级） | 高 |
| 实时查询 | 一次性分析、最新数据 | 较慢（秒级） | 最高 |

### 性能数据

- **缓存构建**: 1000个bbox约3秒
- **缓存查询**: 复杂分析通常<50ms
- **实时查询**: 100个bbox约1秒
- **性能提升**: 缓存比实时快20-100倍

## 最佳实践

### 1. 工作流程建议

```python
# 推荐工作流程
def analysis_workflow():
    # 1. 首次构建缓存
    build_cache(num_bbox=1000, city_filter="boston")
    
    # 2. 进行各种分析
    type_stats = analyze_cached_intersections(group_by=["intersection_type"])
    scene_stats = analyze_cached_intersections(group_by=["scene_token"])
    
    # 3. 根据需要获取详细信息
    details = get_intersection_details(limit=100)
    
    return type_stats, scene_stats, details
```

### 2. 错误处理

```python
try:
    cached_count, stats = build_cache(100, city_filter="boston")
    if cached_count == 0:
        print("未找到匹配的bbox数据，请检查city_filter")
        return
        
    results = analyze_cached_intersections(city_filter="boston")
    if results.empty:
        print("缓存为空，请先构建缓存")
        return
        
except Exception as e:
    logger.error(f"分析失败: {e}")
```

### 3. 数据更新策略

```python
# 定期更新缓存
def update_cache_daily():
    cities = ["boston", "singapore", "pittsburgh"]
    
    for city in cities:
        try:
            # 强制重建每个城市的缓存
            count, _ = build_cache(
                num_bbox=1000,
                city_filter=city,
                force_rebuild=True
            )
            logger.info(f"{city}: 更新了{count}条缓存记录")
        except Exception as e:
            logger.error(f"{city}: 缓存更新失败 - {e}")
```

## 数据库表结构

缓存表 `bbox_intersection_cache` 的结构：

```sql
-- PostgreSQL 语法
CREATE TABLE bbox_intersection_cache (
    id SERIAL PRIMARY KEY,
    scene_token VARCHAR(255) NOT NULL,
    city_id VARCHAR(100),
    intersection_id BIGINT NOT NULL,
    intersectiontype INTEGER,
    intersection_geometry TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_scene_intersection UNIQUE (scene_token, intersection_id)
);

-- 创建索引
CREATE INDEX idx_scene_token ON bbox_intersection_cache (scene_token);
CREATE INDEX idx_city_id ON bbox_intersection_cache (city_id);
CREATE INDEX idx_intersectiontype ON bbox_intersection_cache (intersectiontype);
CREATE INDEX idx_intersection_id ON bbox_intersection_cache (intersection_id);
```

## 常见问题

### Q: 缓存占用多少存储空间？
A: 每条相交记录约200-300字节，1000个bbox通常产生几千到几万条记录，占用几MB到几十MB。

### Q: 如何处理数据更新？
A: 使用 `force_rebuild=True` 重新构建缓存，或者在应用层实现增量更新逻辑。

### Q: 可以同时启用缓存和实时查询吗？
A: 可以。设置 `enable_cache_table=True` 后，仍可使用 `polygon_intersect()` 进行实时查询。

### Q: 如何监控缓存性能？
A: 查看构建统计信息，监控查询响应时间，定期检查缓存命中率。 