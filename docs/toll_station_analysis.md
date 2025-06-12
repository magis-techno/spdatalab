# 收费站轨迹分析功能

## 概述

这个模块提供了专门针对收费站（intersectiontype=2）的轨迹数据分析功能。它可以：

1. **查找收费站数据**：识别所有intersectiontype=2的路口
2. **空间分析**：基于收费站几何范围（可选缓冲区）查询轨迹数据
3. **数据聚合**：按dataset_name对轨迹数据进行聚合统计
4. **可视化支持**：导出QGIS兼容的视图用于地图可视化

## 核心特性

### 🎯 精准定位收费站
- 基于`intersectiontype=2`筛选收费站
- 支持城市级别的过滤
- 自动生成分析用缓冲区

### 📊 轨迹数据分析
- 查询`public.ddi_data_points`表中的轨迹数据
- 按`dataset_name`进行聚合统计
- 计算工作阶段比例（workstage=2）
- 统计轨迹数量、数据点数量等指标

### 🗺️ 可视化支持
- 自动创建QGIS兼容的数据库视图
- 支持收费站位置和轨迹密度可视化
- 提供几何数据用于地图展示

## 快速开始

### 命令行使用

```bash
# 基础分析 - 查找所有收费站
spdatalab analyze-toll-stations

# 限制分析收费站数量
spdatalab analyze-toll-stations --limit 1000

# 自定义缓冲区距离并导出QGIS视图
spdatalab analyze-toll-stations --buffer-distance 200 --export-qgis

# 查看分析汇总
spdatalab toll-stations-summary --analysis-id toll_station_20231201_143022
```

### Python API使用

```python
from spdatalab.fusion.toll_station_analysis import analyze_toll_station_trajectories

# 一站式分析
toll_stations, trajectory_results, analysis_id = analyze_toll_station_trajectories(
    limit=1000,
    use_buffer=True,
    buffer_distance_meters=100.0
)

print(f"找到 {len(toll_stations)} 个收费站")
print(f"分析了 {len(trajectory_results)} 个数据集-收费站组合")
```

## 详细功能说明

### 1. 收费站发现

系统通过以下步骤发现收费站：

1. **直接查询**：直接从`full_intersection`表查找`intersectiontype=2`的收费站
2. **几何处理**：为每个收费站生成缓冲区几何（可选）
3. **数据存储**：将收费站信息存储到`toll_station_analysis`表

### 2. 轨迹数据分析

对于每个发现的收费站：

1. **空间查询**：在`public.ddi_data_points`表中查找相交的轨迹点
2. **数据聚合**：按`dataset_name`分组统计
3. **质量评估**：计算工作阶段2的数据比例
4. **结果存储**：将统计结果保存到`toll_station_trajectories`表

### 3. 分析配置

```python
from spdatalab.fusion.toll_station_analysis import TollStationAnalysisConfig

config = TollStationAnalysisConfig(
    toll_station_type=2,              # intersectiontype值
    buffer_distance_meters=100.0,     # 缓冲区距离
    max_trajectory_records=10000       # 最大轨迹记录数
)
```

## 数据库表结构

### 收费站表 (toll_station_analysis)

| 字段 | 类型 | 说明 |
|------|------|------|
| id | SERIAL | 主键 |
| analysis_id | VARCHAR(100) | 分析ID |
| intersection_id | BIGINT | 路口ID |
| intersectiontype | INTEGER | 路口类型（2=收费站） |
| intersectionsubtype | INTEGER | 路口子类型 |
| intersection_geometry | TEXT | 原始几何（WKT格式） |
| buffered_geometry | TEXT | 缓冲区几何（WKT格式） |
| created_at | TIMESTAMP | 创建时间 |

### 轨迹结果表 (toll_station_trajectories)

| 字段 | 类型 | 说明 |
|------|------|------|
| id | SERIAL | 主键 |
| analysis_id | VARCHAR(100) | 分析ID |
| toll_station_id | BIGINT | 收费站ID |
| dataset_name | VARCHAR(255) | 数据集名称 |
| trajectory_count | INTEGER | 轨迹数量 |
| point_count | INTEGER | 数据点数量 |
| min_timestamp | BIGINT | 最早时间戳 |
| max_timestamp | BIGINT | 最晚时间戳 |
| workstage_2_count | INTEGER | 工作阶段2数量 |
| workstage_2_ratio | FLOAT | 工作阶段2比例 |
| created_at | TIMESTAMP | 创建时间 |

## QGIS可视化

### 连接设置

1. **数据库类型**：PostgreSQL
2. **主机**：localhost (或local_pg)
3. **端口**：5432
4. **数据库**：postgres
5. **用户名**：postgres

### 视图说明

系统会自动创建以下视图：

- **qgis_toll_stations_[analysis_id]**：收费站位置图层
- **qgis_trajectories_[analysis_id]**：轨迹统计图层

### 可视化建议

1. **收费站图层**：
   - 使用点符号显示收费站位置
   - 符号大小可以表示轨迹密度
   - 颜色可以区分不同城市

2. **轨迹统计图层**：
   - 使用热力图显示轨迹密度
   - 不同颜色表示不同的dataset_name
   - 透明度表示工作阶段2的质量比例

## 高级用法

### 自定义分析器

```python
from spdatalab.fusion.toll_station_analysis import TollStationAnalyzer, TollStationAnalysisConfig

# 自定义配置
config = TollStationAnalysisConfig(
    buffer_distance_meters=200.0,
    max_trajectory_records=20000
)

analyzer = TollStationAnalyzer(config)

# 分步执行
toll_stations, analysis_id = analyzer.find_toll_stations(
    num_bbox=1000,
    city_filter="beijing"
)

trajectory_results = analyzer.analyze_trajectories_in_toll_stations(
    analysis_id=analysis_id,
    use_buffer=True
)

# 导出可视化
export_info = analyzer.export_results_for_qgis(analysis_id)
```

### 批量城市分析

```python
cities = ["shanghai", "beijing", "shenzhen", "guangzhou"]
results = {}

for city in cities:
    toll_stations, trajectory_results, analysis_id = analyze_toll_station_trajectories(
        num_bbox=500,
        city_filter=city,
        use_buffer=True
    )
    
    results[city] = {
        'analysis_id': analysis_id,
        'toll_stations': len(toll_stations),
        'trajectory_results': len(trajectory_results)
    }

# 比较不同城市的收费站密度
for city, data in results.items():
    print(f"{city}: {data['toll_stations']} 个收费站")
```

## 性能优化

### 数据库优化

1. **索引使用**：系统自动创建必要的索引
2. **批量处理**：大规模数据采用分块查询
3. **缓存机制**：使用spatial_join_production的缓存功能

### 查询优化

1. **空间索引**：利用PostGIS的GIST索引
2. **几何简化**：可选择使用简化的几何对象
3. **数据过滤**：通过城市、时间等条件预过滤

## 故障排除

### 常见问题

1. **未找到收费站数据**
   - 检查full_intersection表是否存在
   - 确认表中有intersectiontype=2的数据
   - 验证远程数据库连接

2. **轨迹查询缓慢**
   - 减小缓冲区距离
   - 限制最大轨迹记录数
   - 使用城市过滤

3. **QGIS视图创建失败**
   - 检查local_pg数据库权限
   - 确认PostGIS扩展已安装
   - 验证几何数据格式

### 调试建议

```bash
# 开启详细日志
export SPDATALAB_LOG_LEVEL=DEBUG

# 使用调试模式
spdatalab analyze-toll-stations --debug --num-bbox 100
```

## 示例和模板

参考文件：
- `examples/toll_station_analysis_example.py` - 完整示例代码
- `src/spdatalab/fusion/toll_station_analysis.py` - 核心实现
- `src/spdatalab/cli.py` - 命令行接口

## 技术架构

```
收费站分析模块架构：

┌─────────────────────┐
│   CLI Interface     │ 命令行接口
├─────────────────────┤
│  TollStationAnalyzer│ 核心分析器
├─────────────────────┤
│ ProductionSpatialJoin│ 空间连接基础
├─────────────────────┤
│   Database Layer    │ 数据库层
└─────────────────────┘
│                     │
▼                     ▼
local_pg            remote_db
(结果存储)           (源数据查询)
```

这个功能完全集成了现有的空间分析基础设施，提供了专门针对收费站场景的高效分析工具。 