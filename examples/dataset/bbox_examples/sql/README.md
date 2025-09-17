# BBox叠置分析SQL脚本

这个目录包含了bbox叠置分析的核心SQL脚本，可以独立使用或通过Python脚本调用。

## 📋 脚本列表

### 1. `create_analysis_tables.sql`
**用途**: 创建分析结果存储表

**功能**:
- 创建 `bbox_overlap_analysis_results` 表
- 添加PostGIS几何列
- 创建性能优化索引
- 添加数据约束和注释

**执行方式**:
```bash
psql -d postgres -f create_analysis_tables.sql
```

**表结构**:
```sql
bbox_overlap_analysis_results (
    id SERIAL PRIMARY KEY,
    analysis_id VARCHAR(100),          -- 分析批次ID
    hotspot_rank INTEGER,              -- 热点排名
    overlap_count INTEGER,             -- 重叠数量
    total_overlap_area NUMERIC,        -- 总重叠面积
    subdataset_count INTEGER,          -- 涉及子数据集数
    scene_count INTEGER,               -- 涉及场景数
    involved_subdatasets TEXT[],       -- 子数据集列表
    involved_scenes TEXT[],            -- 场景token列表
    geometry GEOMETRY(GEOMETRY,4326),  -- 重叠几何形状
    analysis_params TEXT,              -- JSON格式参数
    created_at TIMESTAMP               -- 创建时间
)
```

### 2. `overlap_analysis.sql`
**用途**: 执行核心的叠置分析查询

**分析逻辑**:
1. **数据质量过滤**: 只分析 `all_good = true` 的高质量数据
2. **城市约束**: 只分析 `city_id` 相同的bbox对，避免跨城市无意义重叠
3. **重叠检测**: 使用 `ST_Intersects` 找出空间相交的bbox对
4. **面积计算**: 使用 `ST_Intersection` 计算重叠面积和比例
5. **热点聚合**: 基于空间网格聚合相邻重叠区域
6. **统计排序**: 按重叠数量和面积排序生成热点排名

**参数模板**:
```sql
-- 需要替换的参数
{unified_view}      -- 统一视图名: clips_bbox_unified_qgis
{analysis_table}    -- 结果表名: bbox_overlap_analysis_results  
{analysis_id}       -- 分析ID: 'overlap_20241201_143022'
{where_clause}      -- 过滤条件: 'AND a.city_id = "beijing"'
{min_overlap_area}  -- 面积阈值: 0.0001
{top_n}            -- 结果数量: 20
```

**执行示例**:
```bash
# 1. 复制文件并编辑参数
cp overlap_analysis.sql my_analysis.sql
# 2. 替换参数 (使用文本编辑器)
# 3. 执行分析
psql -d postgres -f my_analysis.sql
```

**Python调用示例**:
```python
with open('overlap_analysis.sql', 'r') as f:
    sql_template = f.read()

sql = sql_template.format(
    unified_view='clips_bbox_unified_qgis',
    analysis_table='bbox_overlap_analysis_results',
    analysis_id='my_analysis_001',
    where_clause='AND a.city_id = \'beijing\'',
    min_overlap_area=0.0001,
    top_n=20
)

engine.execute(text(sql))
```

### 3. `qgis_views.sql`
**用途**: 创建QGIS兼容的可视化视图

**创建的视图**:

#### `qgis_bbox_overlap_hotspots`
- **用途**: 主要的热点展示视图
- **特点**: 包含密度分级、格式化标签
- **QGIS设置**: 主键=`qgis_id`, 几何列=`geometry`

#### `qgis_bbox_overlap_summary`  
- **用途**: 分析批次汇总统计
- **特点**: 按analysis_id聚合的整体指标
- **QGIS设置**: 显示分析覆盖范围

#### `qgis_bbox_overlap_details`
- **用途**: 详细的热点信息视图
- **特点**: 包含几何度量、复杂性指标
- **QGIS设置**: 用于深入分析

**执行方式**:
```bash
psql -d postgres -f qgis_views.sql
```

## 🔧 自定义分析

### 修改重叠检测逻辑

在 `overlap_analysis.sql` 中修改 `overlapping_pairs` CTE:

```sql
-- 原始: 任何重叠
WHERE ST_Intersects(a.geometry, b.geometry)

-- 修改: 只要实质性重叠 (>50%面积)
WHERE ST_Intersects(a.geometry, b.geometry)
  AND ST_Area(ST_Intersection(a.geometry, b.geometry)) > 
      0.5 * LEAST(ST_Area(a.geometry), ST_Area(b.geometry))

-- 修改: 只要中心点重叠
WHERE ST_Contains(a.geometry, ST_Centroid(b.geometry))
   OR ST_Contains(b.geometry, ST_Centroid(a.geometry))
```

### 添加时间维度分析

```sql
-- 在 overlapping_pairs CTE 中添加时间条件
WHERE ST_Intersects(a.geometry, b.geometry)
  AND ABS(a.timestamp - b.timestamp) <= 3600  -- 1小时内的重叠
```

### 修改热点聚合策略

```sql
-- 原始: 基于网格聚合
GROUP BY ST_SnapToGrid(ST_Centroid(overlap_geometry), 0.001)

-- 修改: 基于行政区聚合 (需要额外的行政区表)
GROUP BY admin_region.name
FROM overlapping_pairs op
JOIN admin_regions admin_region ON 
    ST_Within(ST_Centroid(op.overlap_geometry), admin_region.geometry)

-- 修改: 基于距离聚合
GROUP BY ST_ClusterDBSCAN(ST_Centroid(overlap_geometry), 0.01, 2) OVER ()
```

### 添加质量指标

在 `final_hotspots` CTE 中添加:

```sql
-- 重叠质量评分
CASE 
    WHEN avg_max_overlap_ratio > 0.8 THEN 'High Quality'
    WHEN avg_max_overlap_ratio > 0.5 THEN 'Medium Quality'
    ELSE 'Low Quality'
END as overlap_quality,

-- 空间分散度
ST_Area(ST_ConvexHull(hotspot_geometry)) / ST_Area(hotspot_geometry) as dispersion_ratio,

-- 形状复杂度
ST_Perimeter(hotspot_geometry) / (2 * SQRT(PI() * ST_Area(hotspot_geometry))) as shape_complexity
```

## 📊 性能优化

### 索引优化

确保以下索引存在:
```sql
-- 空间索引 (最重要)
CREATE INDEX IF NOT EXISTS idx_unified_view_geom 
ON clips_bbox_unified_qgis USING GIST (geometry);

-- 过滤字段索引
CREATE INDEX IF NOT EXISTS idx_unified_view_city 
ON clips_bbox_unified_qgis (city_id);

CREATE INDEX IF NOT EXISTS idx_unified_view_subdataset 
ON clips_bbox_unified_qgis (subdataset_name);

-- 复合索引
CREATE INDEX IF NOT EXISTS idx_unified_view_city_geom 
ON clips_bbox_unified_qgis (city_id) 
INCLUDE (geometry);
```

### 查询优化

1. **分区处理**:
```sql
-- 按城市分区处理大数据集
CREATE TEMP TABLE beijing_bbox AS
SELECT * FROM clips_bbox_unified_qgis 
WHERE city_id = 'beijing';

-- 然后在分区表上运行分析
```

2. **并行处理**:
```sql
-- 设置并行度
SET max_parallel_workers_per_gather = 4;
SET parallel_tuple_cost = 0.01;
```

3. **内存优化**:
```sql
-- 增加工作内存
SET work_mem = '256MB';
SET shared_buffers = '1GB';
```

## 🐛 故障排除

### 常见错误

1. **几何数据无效**:
```sql
-- 检查和修复
SELECT count(*) FROM clips_bbox_unified_qgis 
WHERE NOT ST_IsValid(geometry);

UPDATE clips_bbox_unified_qgis 
SET geometry = ST_MakeValid(geometry)
WHERE NOT ST_IsValid(geometry);
```

2. **内存不足**:
```sql
-- 减少数据量
WHERE ST_Area(geometry) > 0.0001  -- 过滤小bbox

-- 或分批处理
LIMIT 10000 OFFSET 0;
```

3. **查询超时**:
```sql
-- 增加超时时间
SET statement_timeout = '10min';

-- 或简化查询
-- 移除复杂的几何运算
```

### 调试技巧

1. **分步执行**:
```sql
-- 只执行第一个CTE查看中间结果
WITH overlapping_pairs AS (...)
SELECT count(*), avg(overlap_area) 
FROM overlapping_pairs;
```

2. **空间查询可视化**:
```sql
-- 导出中间几何结果查看
SELECT ST_AsGeoJSON(overlap_geometry) 
FROM overlapping_pairs 
LIMIT 10;
```

3. **性能分析**:
```sql
-- 查看执行计划
EXPLAIN (ANALYZE, BUFFERS) 
WITH overlapping_pairs AS (...) 
SELECT ... FROM final_hotspots;
```

## 🔄 版本历史

- **v1.0** (2024-12): 初始版本，基础重叠分析
- **v1.1**: 计划添加时间维度支持
- **v1.2**: 计划添加更多几何度量指标

---

**注意**: 所有SQL脚本都假设使用PostGIS扩展，请确保数据库已正确安装PostGIS。
