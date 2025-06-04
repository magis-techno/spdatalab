# 轨迹交集分析功能使用指南

本指南介绍如何使用新开发的轨迹intersection overlay分析功能。

## 功能概述

新的交集分析模块提供了以下核心功能：

### 1. 轨迹交集分析器 (TrajectoryIntersectionAnalyzer)
- **轨迹与路口交集分析**: 分析轨迹数据与路口的空间交集关系
- **轨迹与道路交集分析**: 分析轨迹数据与道路网络的重叠情况
- **轨迹与区域交集分析**: 分析轨迹数据与特定区域的覆盖关系
- **轨迹间交集分析**: 分析不同轨迹之间的时空交集

### 2. 通用叠置分析器 (OverlayAnalyzer)
- **点在面内分析**: 点要素与面要素的包含关系分析
- **线面交集分析**: 线要素与面要素的相交分析
- **面面叠置分析**: 多种面要素叠置操作（交集、并集、差集等）
- **缓冲区分析**: 生成缓冲区并进行叠置分析
- **邻近分析**: K近邻和距离阈值邻近分析

### 3. 高级交集处理器 (IntersectionProcessor)
- **批量工作流程**: 一站式完成多种交集分析
- **并行处理**: 支持多城市并行分析
- **结果质量评估**: 自动评估分析结果质量
- **多格式导出**: 支持CSV、GeoJSON、GPKG等多种格式
- **可视化报告**: 自动生成统计图表和分析报告

## 快速开始

### 1. 基础轨迹交集分析

```python
from spdatalab.fusion import TrajectoryIntersectionAnalyzer

# 初始化分析器
analyzer = TrajectoryIntersectionAnalyzer()

# 分析轨迹与路口的交集
junction_results = analyzer.analyze_trajectory_intersection_with_junctions(
    trajectory_table="clips_bbox",
    junction_table="intersections", 
    buffer_meters=25.0,
    output_table="trajectory_junction_results"
)

print(f"找到 {len(junction_results)} 个轨迹-路口交集")
```

### 2. 轨迹间交集分析

```python
# 分析轨迹间的时空交集
traj_results = analyzer.analyze_trajectory_to_trajectory_intersection(
    trajectory_table1="clips_bbox",
    buffer_meters=10.0,
    time_tolerance_seconds=1800,  # 30分钟内
    output_table="trajectory_intersections"
)

# 查看交集类型分布
print(traj_results['intersection_type'].value_counts())
```

### 3. 综合分析工作流程

```python
from spdatalab.fusion import IntersectionProcessor

# 初始化处理器
processor = IntersectionProcessor(max_workers=4)

# 配置综合分析
analysis_config = {
    'trajectory_junction_analysis': {
        'enabled': True,
        'trajectory_table': 'clips_bbox',
        'junction_table': 'intersections',
        'buffer_meters': 20.0
    },
    'trajectory_to_trajectory_analysis': {
        'enabled': True,
        'trajectory_table1': 'clips_bbox',
        'buffer_meters': 8.0,
        'time_tolerance_seconds': 600
    },
    'generate_visualizations': True
}

# 运行综合分析
results = processor.run_comprehensive_intersection_analysis(
    analysis_config=analysis_config,
    output_dir="data/intersection_results",
    export_formats=['csv', 'geojson', 'gpkg']
)
```

## 命令行使用

### 1. 基础交集分析

```bash
# 轨迹与路口交集分析
python -m spdatalab analyze-intersection \
    --analysis-type trajectory-junction \
    --trajectory-table clips_bbox \
    --target-table intersections \
    --buffer-meters 25.0 \
    --output-dir data/junction_analysis \
    --export-formats csv geojson \
    --quality-check

# 轨迹间交集分析
python -m spdatalab analyze-intersection \
    --analysis-type trajectory-to-trajectory \
    --trajectory-table clips_bbox \
    --buffer-meters 10.0 \
    --time-tolerance 1800 \
    --output-dir data/trajectory_analysis
```

### 2. 综合分析

```bash
# 运行综合分析
python -m spdatalab analyze-intersection \
    --analysis-type comprehensive \
    --trajectory-table clips_bbox \
    --buffer-meters 20.0 \
    --output-dir data/comprehensive_analysis \
    --export-formats csv geojson gpkg \
    --max-workers 4 \
    --quality-check
```

### 3. 批量分析

```bash
# 从配置文件批量运行
python -m spdatalab batch-intersection-analysis \
    --config-file config/analysis_batch.json \
    --output-dir data/batch_results \
    --max-workers 6
```

批量分析配置文件示例 (`config/analysis_batch.json`):

```json
{
  "analyses": [
    {
      "name": "junction_analysis_20m",
      "type": "comprehensive",
      "config": {
        "trajectory_junction_analysis": {
          "enabled": true,
          "trajectory_table": "clips_bbox",
          "junction_table": "intersections",
          "buffer_meters": 20.0
        },
        "generate_visualizations": true
      },
      "export_formats": ["csv", "geojson"]
    },
    {
      "name": "junction_analysis_50m", 
      "type": "comprehensive",
      "config": {
        "trajectory_junction_analysis": {
          "enabled": true,
          "trajectory_table": "clips_bbox",
          "junction_table": "intersections", 
          "buffer_meters": 50.0
        }
      },
      "export_formats": ["csv", "gpkg"]
    }
  ]
}
```

## 高级功能

### 1. 并行处理

对于大规模数据，可以按城市进行并行处理：

```python
# 并行分析多个城市
city_ids = ['beijing', 'shanghai', 'guangzhou']

parallel_results = processor.run_parallel_intersection_analysis(
    city_ids=city_ids,
    analysis_type='trajectory_junction',
    analysis_params={
        'trajectory_table': 'clips_bbox',
        'junction_table': 'intersections',
        'buffer_meters': 25.0
    },
    output_dir="data/parallel_results"
)
```

### 2. 结果质量评估

```python
# 评估分析结果质量
quality_report = processor.evaluate_intersection_quality(
    intersection_results=junction_results,
    quality_thresholds={
        'min_intersection_area_m2': 2.0,
        'max_distance_meters': 500.0,
        'min_intersection_count': 10
    }
)

print(f"质量得分: {quality_report['quality_score']}")
print(f"质量等级: {quality_report['quality_level']}")

# 查看改进建议
for recommendation in quality_report['recommendations']:
    print(f"建议: {recommendation}")
```

### 3. 通用叠置分析

```python
from spdatalab.fusion import OverlayAnalyzer

overlay_analyzer = OverlayAnalyzer()

# 缓冲区分析
buffer_results = overlay_analyzer.buffer_analysis(
    input_table="clips_bbox",
    buffer_distance_meters=50.0,
    dissolve=True,  # 合并重叠的缓冲区
    output_table="trajectory_buffers"
)

# 邻近分析 - 找每个轨迹最近的3个路口
proximity_results = overlay_analyzer.proximity_analysis(
    target_table="clips_bbox",
    reference_table="intersections",
    max_distance_meters=200.0,
    k_nearest=3,
    output_table="trajectory_proximity"
)

# 面面叠置分析
overlay_results = overlay_analyzer.polygon_polygon_overlay_analysis(
    polygon_table1="trajectory_buffers",
    polygon_table2="city_regions", 
    overlay_type="intersection",  # 可选: union, difference, symmetric_difference
    min_area_threshold_m2=10.0
)
```

## SQL视图和函数

系统还提供了一系列SQL视图和函数来支持交集分析：

### 1. 物化视图

```sql
-- 刷新所有交集分析相关的物化视图
SELECT refresh_intersection_materialized_views();

-- 查询轨迹与路口交集结果
SELECT * FROM mv_trajectory_junction_intersections 
WHERE city_id = 'beijing' AND distance_meters < 50;

-- 查询轨迹密度分析结果
SELECT * FROM mv_trajectory_density_analysis
WHERE density_category = 'high_density';

-- 查询轨迹时空交集
SELECT * FROM mv_trajectory_spatiotemporal_intersections
WHERE temporal_category = 'simultaneous';
```

### 2. 分析函数

```sql
-- 轨迹热点分析
SELECT * FROM analyze_trajectory_hotspots(
    p_city_id => 'beijing',
    p_buffer_meters => 50.0,
    p_min_trajectory_count => 10
);

-- 轨迹覆盖度分析
SELECT * FROM analyze_trajectory_coverage(
    p_city_id => 'beijing',
    p_reference_table => 'intersections',
    p_buffer_meters => 30.0
);
```

### 3. 汇总视图

```sql
-- 查看交集分析汇总统计
SELECT * FROM v_intersection_analysis_summary;

-- 查看数据质量报告
SELECT * FROM v_intersection_data_quality;
```

## 输出格式说明

### 1. 交集分析结果字段

轨迹与路口交集分析输出字段：
- `scene_token`: 场景标识
- `data_name`: 数据名称
- `city_id`: 城市ID
- `inter_id`: 路口ID
- `inter_type`: 路口类型
- `distance_meters`: 距离（米）
- `intersection_area_m2`: 交集面积（平方米）
- `intersection_type`: 交集类型（直接交集/缓冲区交集）
- `intersection_point`: 交集点几何

### 2. 导出文件

支持的导出格式：
- **CSV**: 表格数据，不含几何信息
- **GeoJSON**: 标准地理数据格式，便于Web应用
- **GPKG**: OGC标准，支持复杂几何和属性
- **Shapefile**: 传统GIS格式

### 3. 可视化报告

自动生成的可视化包括：
- 交集数量分布图
- 面积/距离分布直方图
- 交集类型饼图
- 城市间对比图

## 性能优化建议

### 1. 数据库优化

```sql
-- 确保几何列有空间索引
CREATE INDEX IF NOT EXISTS idx_clips_bbox_geometry 
    ON clips_bbox USING GIST(geometry);

CREATE INDEX IF NOT EXISTS idx_intersections_geom 
    ON intersections USING GIST(geom);

-- 为常用查询列创建索引
CREATE INDEX IF NOT EXISTS idx_clips_bbox_city_id 
    ON clips_bbox(city_id);
```

### 2. 分析参数调优

- **缓冲区大小**: 根据数据精度和分析需求调整，过大会影响性能
- **时间容差**: 轨迹间分析时，合理设置时间窗口
- **批次大小**: 大数据集时使用适当的批次处理
- **并行度**: 根据系统资源调整max_workers参数

### 3. 内存管理

```python
# 对于大型数据集，分批处理
analyzer = TrajectoryIntersectionAnalyzer()

# 分城市处理大数据集
for city_id in city_list:
    city_results = analyzer.analyze_trajectory_intersection_with_junctions(
        trajectory_table=f"clips_bbox",
        junction_table="intersections",
        # 添加城市过滤条件到SQL查询中
    )
    # 立即保存结果释放内存
    city_results.to_file(f"results_{city_id}.gpkg")
```

## 故障排除

### 1. 常见错误

**几何无效错误**:
```sql
-- 修复无效几何
UPDATE clips_bbox 
SET geometry = ST_MakeValid(geometry) 
WHERE NOT ST_IsValid(geometry);
```

**投影系统问题**:
```python
# 确保数据使用正确的坐标系
gdf = gdf.to_crs('EPSG:4326')  # 转换为WGS84
```

**内存不足**:
```python
# 使用分块处理
chunk_size = 1000
for chunk in pd.read_sql(sql, engine, chunksize=chunk_size):
    # 处理每个chunk
    process_chunk(chunk)
```

### 2. 性能问题

- 检查空间索引是否存在
- 调整缓冲区大小和批次参数
- 使用并行处理分散计算负载
- 考虑数据预处理和过滤

## 扩展开发

### 1. 自定义分析类型

```python
class CustomIntersectionAnalyzer(TrajectoryIntersectionAnalyzer):
    def analyze_custom_intersection(self, **kwargs):
        # 实现自定义交集分析逻辑
        sql = text("""
            -- 自定义SQL查询
            SELECT ...
        """)
        # 执行并返回结果
        return gpd.read_postgis(sql, self.engine, **kwargs)
```

### 2. 新的叠置操作

```python
class ExtendedOverlayAnalyzer(OverlayAnalyzer):
    def advanced_spatial_operation(self, **kwargs):
        # 实现高级空间操作
        pass
```

### 3. 自定义导出格式

```python
def export_to_custom_format(gdf, output_path):
    # 实现自定义格式导出
    pass

# 在IntersectionProcessor中注册
processor._export_methods['custom'] = export_to_custom_format
```

## 更多示例

完整的使用示例请参考：
- `examples/trajectory_intersection_example.py` - 完整功能演示
- `tests/` 目录下的单元测试 - 各功能模块测试用例

## 技术支持

如有问题或建议，请：
1. 查看日志输出了解详细错误信息
2. 检查数据质量和格式
3. 参考示例代码和文档
4. 提交Issue到项目仓库 