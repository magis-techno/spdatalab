# 集成轨迹分析使用指南

## 概述

集成轨迹分析模块提供了一个统一的入口点，自动执行两阶段轨迹分析流程：

1. **第一阶段：轨迹道路分析** - 基于轨迹膨胀缓冲区查找相关道路元素
2. **第二阶段：轨迹车道分析** - 基于候选道路元素进行精细车道分析

## 系统架构

```
GeoJSON输入 → 轨迹加载 → 道路分析 → 车道分析 → 结果集成 → 报告生成
    ↓              ↓          ↓         ↓         ↓          ↓
输入验证      轨迹记录     candidate   车道分段    综合统计    可视化输出
              解析      road/lane    质量检查     错误汇总     QGIS视图
```

## 快速开始

### 1. 基本使用

```bash
# 最简单的使用方式
python -m spdatalab.fusion.integrated_trajectory_analysis --input trajectories.geojson

# 指定输出路径
python -m spdatalab.fusion.integrated_trajectory_analysis \
  --input trajectories.geojson \
  --output-path ./analysis_results

# 指定分析ID
python -m spdatalab.fusion.integrated_trajectory_analysis \
  --input trajectories.geojson \
  --analysis-id my_analysis_20240101
```

### 2. 编程接口

```python
from spdatalab.fusion.integrated_trajectory_analysis import analyze_trajectories_from_geojson
from spdatalab.fusion.integrated_analysis_config import create_default_config

# 创建配置
config = create_default_config()

# 执行分析
results = analyze_trajectories_from_geojson(
    geojson_file="trajectories.geojson",
    config=config,
    analysis_id="my_analysis"
)

# 检查结果
if results['status'] == 'completed':
    print(f"分析完成: {results['analysis_id']}")
    print(f"总轨迹数: {results['summary']['total_trajectories']}")
    print(f"道路分析成功率: {results['summary']['road_success_rate']}%")
    print(f"车道分析成功率: {results['summary']['lane_success_rate']}%")
else:
    print(f"分析失败: {results['error']}")
```

## 输入格式

### GeoJSON格式要求

输入的GeoJSON文件必须包含以下字段：

- `scene_id`: 场景ID（字符串）
- `data_name`: 数据名称（字符串）
- `geometry`: LineString几何（必须为有效的轨迹线）

### 示例GeoJSON

```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "properties": {
        "scene_id": "scene_001",
        "data_name": "trajectory_data_001",
        "additional_info": "可选的额外信息"
      },
      "geometry": {
        "type": "LineString",
        "coordinates": [
          [116.3974, 39.9093],
          [116.3975, 39.9094],
          [116.3976, 39.9095]
        ]
      }
    },
    {
      "type": "Feature",
      "properties": {
        "scene_id": "scene_002",
        "data_name": "trajectory_data_002"
      },
      "geometry": {
        "type": "LineString",
        "coordinates": [
          [116.4074, 39.9193],
          [116.4075, 39.9194],
          [116.4076, 39.9195]
        ]
      }
    }
  ]
}
```

### 从trajectory.py输出创建GeoJSON

如果你已经有使用`trajectory.py`创建的轨迹表，可以使用以下SQL将其导出为GeoJSON：

```sql
-- 导出轨迹为GeoJSON格式
SELECT jsonb_build_object(
    'type', 'FeatureCollection',
    'features', jsonb_agg(feature)
) as geojson_output
FROM (
    SELECT jsonb_build_object(
        'type', 'Feature',
        'properties', jsonb_build_object(
            'scene_id', scene_id,
            'data_name', data_name,
            'start_time', start_time,
            'end_time', end_time,
            'avg_speed', avg_speed
        ),
        'geometry', ST_AsGeoJSON(geometry)::jsonb
    ) as feature
    FROM your_trajectory_table
    WHERE geometry IS NOT NULL
) features;
```

## 配置管理

### 配置预设

系统提供三种预设配置：

1. **默认配置** (`default`) - 平衡性能和精度
2. **快速配置** (`fast`) - 优化速度，略降精度
3. **高精度配置** (`high_precision`) - 最高精度，较慢速度

```bash
# 使用快速配置
python -m spdatalab.fusion.integrated_trajectory_analysis \
  --input trajectories.geojson \
  --config-preset fast

# 使用高精度配置
python -m spdatalab.fusion.integrated_trajectory_analysis \
  --input trajectories.geojson \
  --config-preset high_precision
```

### 配置文件

#### 创建配置文件

```python
from spdatalab.fusion.integrated_analysis_config import create_default_config

# 创建并保存配置
config = create_default_config()
config.save_to_file("my_config.json")
```

#### 配置文件示例

```json
{
  "analysis_name": "我的轨迹分析",
  "analysis_description": "详细的轨迹分析项目",
  "road_analysis_config": {
    "buffer_distance": 3.0,
    "forward_chain_limit": 500.0,
    "backward_chain_limit": 100.0,
    "max_recursion_depth": 10
  },
  "lane_analysis_config": {
    "sampling_strategy": "distance",
    "distance_interval": 10.0,
    "buffer_distance": 2.0,
    "window_size": 5,
    "min_single_lane_points": 5
  },
  "batch_processing_config": {
    "enable_parallel": true,
    "max_workers": 4,
    "road_analysis_batch_size": 20,
    "lane_analysis_batch_size": 10
  },
  "output_config": {
    "generate_reports": true,
    "report_format": "markdown",
    "create_qgis_views": true,
    "export_path": "output"
  }
}
```

#### 使用配置文件

```bash
python -m spdatalab.fusion.integrated_trajectory_analysis \
  --input trajectories.geojson \
  --config-file my_config.json
```

### 命令行参数覆盖

```bash
# 覆盖缓冲区距离
python -m spdatalab.fusion.integrated_trajectory_analysis \
  --input trajectories.geojson \
  --road-buffer-distance 5.0 \
  --lane-buffer-distance 3.0

# 覆盖采样策略
python -m spdatalab.fusion.integrated_trajectory_analysis \
  --input trajectories.geojson \
  --sampling-strategy time \
  --time-interval 1.5

# 控制输出
python -m spdatalab.fusion.integrated_trajectory_analysis \
  --input trajectories.geojson \
  --no-reports \
  --export-geojson \
  --export-parquet
```

## 使用示例

### 1. 批量分析多个轨迹

```python
import json
from spdatalab.fusion.integrated_trajectory_analysis import analyze_trajectories_from_geojson
from spdatalab.fusion.integrated_analysis_config import create_fast_config

# 创建快速配置
config = create_fast_config()
config.output_config.export_path = "batch_analysis_results"

# 分析多个文件
geojson_files = [
    "trajectories_2024_01.geojson",
    "trajectories_2024_02.geojson",
    "trajectories_2024_03.geojson"
]

results = []
for file in geojson_files:
    print(f"分析文件: {file}")
    
    result = analyze_trajectories_from_geojson(
        geojson_file=file,
        config=config,
        analysis_id=f"batch_{file.split('.')[0]}"
    )
    
    results.append(result)
    
    # 输出进度
    if result['status'] == 'completed':
        summary = result['summary']
        print(f"  ✓ 完成: {summary['total_trajectories']} 轨迹, "
              f"道路分析 {summary['road_success_rate']}%, "
              f"车道分析 {summary['lane_success_rate']}%")
    else:
        print(f"  ✗ 失败: {result['error']}")

# 汇总统计
total_trajectories = sum(r['summary']['total_trajectories'] for r in results if r['status'] == 'completed')
print(f"\n批量分析完成，总轨迹数: {total_trajectories}")
```

### 2. 自定义分析流程

```python
from spdatalab.fusion.integrated_trajectory_analysis import IntegratedTrajectoryAnalyzer
from spdatalab.fusion.integrated_analysis_config import create_default_config

# 创建自定义配置
config = create_default_config()
config.road_analysis_config.buffer_distance = 5.0  # 增加道路缓冲区
config.lane_analysis_config.sampling_strategy = "time"  # 使用时间采样
config.lane_analysis_config.time_interval = 1.0  # 1秒采样间隔
config.output_config.generate_reports = True
config.output_config.create_qgis_views = True

# 创建分析器
analyzer = IntegratedTrajectoryAnalyzer(config)

# 执行分析
results = analyzer.analyze_trajectories_from_geojson(
    geojson_file="custom_trajectories.geojson",
    analysis_id="custom_analysis_001"
)

# 处理结果
if results['status'] == 'completed':
    # 访问详细结果
    road_results = results['road_analysis_results']
    lane_results = results['lane_analysis_results']
    
    print(f"道路分析结果数: {len(road_results)}")
    print(f"车道分析结果数: {len(lane_results)}")
    
    # 分析失败的轨迹
    for trajectory_id, analysis_id, summary in road_results:
        if analysis_id is None:
            print(f"道路分析失败: {trajectory_id} - {summary['error']}")
    
    for trajectory_id, analysis_id, summary in lane_results:
        if analysis_id is None:
            print(f"车道分析失败: {trajectory_id} - {summary['error']}")
```

### 3. 结果导出和可视化

```python
import pandas as pd
from pathlib import Path

# 分析结果
results = analyze_trajectories_from_geojson("trajectories.geojson")

if results['status'] == 'completed':
    # 导出详细统计
    summary = results['summary']
    stats_df = pd.DataFrame([{
        'analysis_id': results['analysis_id'],
        'total_trajectories': summary['total_trajectories'],
        'road_success_rate': summary['road_success_rate'],
        'lane_success_rate': summary['lane_success_rate'],
        'total_errors': summary['total_errors'],
        'analysis_duration': results['duration']
    }])
    
    stats_df.to_csv('analysis_summary.csv', index=False)
    
    # 导出错误详情
    if results['errors']:
        errors_df = pd.DataFrame(results['errors'])
        errors_df.to_csv('analysis_errors.csv', index=False)
    
    # 导出轨迹基本信息
    trajectories_df = pd.DataFrame(results['trajectories'])
    trajectories_df.to_csv('trajectories_info.csv', index=False)
    
    print("结果已导出到CSV文件")
```

## 输出结果

### 1. 控制台输出

```
2024-01-01 10:00:00 - integrated_trajectory_analysis - INFO - 开始集成轨迹分析: integrated_20240101_100000
2024-01-01 10:00:00 - integrated_trajectory_analysis - INFO - GeoJSON文件: trajectories.geojson
2024-01-01 10:00:01 - integrated_trajectory_analysis - INFO - ✓ 输入文件格式验证通过
2024-01-01 10:00:01 - integrated_trajectory_analysis - INFO - ✓ 加载轨迹数据: 50 条
2024-01-01 10:00:01 - integrated_trajectory_analysis - INFO - 执行第一阶段：道路分析
2024-01-01 10:00:15 - integrated_trajectory_analysis - INFO - ✓ 道路分析完成: 成功 48, 失败 2
2024-01-01 10:00:15 - integrated_trajectory_analysis - INFO - 执行第二阶段：车道分析
2024-01-01 10:00:30 - integrated_trajectory_analysis - INFO - ✓ 车道分析完成: 成功 45, 失败 3
2024-01-01 10:00:30 - integrated_trajectory_analysis - INFO - 集成轨迹分析完成: integrated_20240101_100000
2024-01-01 10:00:30 - integrated_trajectory_analysis - INFO - 分析完成: 分析ID: integrated_20240101_100000 | 总轨迹数: 50 | 道路分析成功率: 96.0% | 车道分析成功率: 90.0% | 分析时长: 0:00:30
```

### 2. 文件输出

分析完成后，会在指定的输出路径生成以下文件：

```
output/
├── reports/
│   ├── analysis_id_comprehensive.md      # 综合分析报告
│   ├── analysis_id_road_analysis.md      # 道路分析报告
│   └── analysis_id_lane_analysis.md      # 车道分析报告
├── analysis_id_results.json              # 完整分析结果（JSON格式）
└── stats/
    ├── analysis_summary.csv              # 分析摘要统计
    └── trajectory_details.csv            # 轨迹详情统计
```

### 3. 数据库表

分析过程会创建以下数据库表：

#### 道路分析表
- `trajectory_road_analysis` - 主分析表
- `trajectory_road_lanes` - 候选车道表
- `trajectory_road_intersections` - 相交路口表
- `trajectory_road_roads` - 相关道路表

#### 车道分析表
- `trajectory_lane_segments` - 车道分段表
- `trajectory_lane_buffer` - 缓冲区分析表
- `trajectory_quality_check` - 质量检查表

## 性能优化

### 1. 批量处理配置

```python
# 优化批量处理性能
config = create_default_config()
config.batch_processing_config.enable_parallel = True
config.batch_processing_config.max_workers = 8
config.batch_processing_config.road_analysis_batch_size = 50
config.batch_processing_config.lane_analysis_batch_size = 20
```

### 2. 数据库优化

```sql
-- 为数据库表创建索引
CREATE INDEX CONCURRENTLY idx_trajectory_road_lanes_analysis_id 
ON trajectory_road_lanes(analysis_id);

CREATE INDEX CONCURRENTLY idx_trajectory_road_lanes_lane_id 
ON trajectory_road_lanes(lane_id);

CREATE INDEX CONCURRENTLY idx_trajectory_lane_segments_analysis_id 
ON trajectory_lane_segments(analysis_id);

-- 定期清理过期分析结果
DELETE FROM trajectory_road_analysis 
WHERE created_at < NOW() - INTERVAL '30 days';
```

### 3. 内存优化

```python
# 大数据集处理配置
config = create_default_config()
config.road_analysis_config.max_roads_per_query = 50  # 减少单次查询量
config.road_analysis_config.max_lanes_from_roads = 2000  # 限制lane数量
config.lane_analysis_config.batch_size = 5  # 减少批处理大小
```

## 故障排除

### 1. 常见错误

#### 输入文件格式错误
```
错误: GeoJSON格式验证失败: 缺少必要字段: ['scene_id']
解决: 确保GeoJSON文件包含必要的属性字段
```

#### 数据库连接问题
```
错误: 数据库连接失败
解决: 检查数据库配置和网络连接
```

#### 内存不足
```
错误: MemoryError
解决: 减少批处理大小或增加系统内存
```

### 2. 调试模式

```bash
# 启用调试模式
python -m spdatalab.fusion.integrated_trajectory_analysis \
  --input trajectories.geojson \
  --debug \
  --verbose
```

### 3. 演习模式

```bash
# 演习模式（不实际执行）
python -m spdatalab.fusion.integrated_trajectory_analysis \
  --input trajectories.geojson \
  --dry-run \
  --verbose
```

### 4. 日志配置

```python
import logging

# 配置详细日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('trajectory_analysis.log'),
        logging.StreamHandler()
    ]
)
```

## 最佳实践

### 1. 数据准备
- 确保GeoJSON文件格式正确
- 验证轨迹几何的有效性
- 适当的数据量（建议单次不超过1000条轨迹）

### 2. 配置选择
- 小数据集使用高精度配置
- 大数据集使用快速配置
- 生产环境使用默认配置

### 3. 监控和维护
- 定期清理过期的分析结果
- 监控数据库性能
- 备份重要的分析结果

### 4. 错误处理
- 设置合理的重试机制
- 记录详细的错误日志
- 实现优雅的错误恢复

## 常见问题 (FAQ)

### Q1: 如何处理大量轨迹数据？
A1: 建议将大数据集分批处理，每批不超过1000条轨迹，并使用快速配置。

### Q2: 分析结果保存多长时间？
A2: 分析结果默认保存在数据库中，建议定期清理超过30天的历史数据。

### Q3: 如何在QGIS中查看结果？
A3: 分析完成后会自动创建QGIS视图，可以直接在QGIS中连接数据库查看。

### Q4: 车道分析失败但道路分析成功是什么原因？
A4: 这通常是因为候选车道数据不足或轨迹质量不满足车道分析要求。

### Q5: 如何提高分析准确性？
A5: 使用高精度配置，适当增加缓冲区距离，使用更小的采样间隔。

## 版本更新日志

### v1.0.0
- 初始版本发布
- 支持两阶段轨迹分析
- GeoJSON输入支持
- 配置管理系统
- 批量处理功能
- 自动报告生成

---

更多详细信息请参考：
- [轨迹道路分析指南](trajectory_road_analysis_guide.md)
- [轨迹车道分析指南](trajectory_lane_analysis_guide.md)
- [空间连接分析指南](spatial_join_usage_guide.md) 