# 轨迹道路分析模块使用指南

## 概述

轨迹道路分析模块（`trajectory_road_analysis`）是一个基于自车轨迹进行道路元素分析的工具。该模块能够根据轨迹数据查找相关的lane、intersection、road等道路元素，并提供完整的空间关联分析。

## 功能特性

### 核心功能
1. **轨迹膨胀**：将轨迹膨胀3m生成缓冲区
2. **空间相交分析**：查找与轨迹相交的lane和intersection
3. **关联扩展**：
   - 补齐intersection的inlane和outlane
   - 基于road_id扩展所有相关lane
   - 基于lanenextlane关系扩展前后lane链路
4. **道路信息收集**：收集所有相关的road信息
5. **QGIS可视化**：导出结果供QGIS查看

### 分析层次
- **直接相交**：与轨迹缓冲区直接相交的lane
- **路口相关**：intersection的inlane和outlane
- **道路相关**：同road_id的其他lane
- **前向链路**：向前不超过500m的lane链路
- **后向链路**：向后不超过100m的lane链路

## 安装和配置

### 依赖要求
```python
# 主要依赖
pandas>=1.5.0
sqlalchemy>=2.0.0
psycopg>=3.0.0
geopandas>=0.12.0
```

### 数据库配置
模块需要连接两个数据库：
- **本地数据库**：存储分析结果
- **远程数据库**：查询原始道路数据

```python
from spdatalab.fusion.trajectory_road_analysis import TrajectoryRoadAnalysisConfig

config = TrajectoryRoadAnalysisConfig(
    local_dsn="postgresql+psycopg://user:pass@localhost:5432/local_db",
    remote_dsn="postgresql+psycopg://user:pass@remote:5432/remote_db"
)
```

#### 数据库连接优化

模块包含了多项数据库连接优化：

1. **连接池配置**：
   - 连接池大小：5个连接
   - 最大溢出连接：10个
   - 连接回收时间：3600秒
   - 连接预检查：启用

2. **查询超时设置**：
   - 普通查询超时：60秒
   - 递归查询超时：120秒
   - 连接超时：30秒

3. **查询结果限制**：
   - Lane查询限制：1000条
   - Intersection查询限制：100条
   - 前向链路限制：500条
   - 后向链路限制：200条

## 使用方法

### 1. 基本使用

```python
from spdatalab.fusion.trajectory_road_analysis import analyze_trajectory_road_elements

# 准备轨迹数据
trajectory_id = "my_trajectory_001"
trajectory_wkt = "LINESTRING(116.3 39.9, 116.31 39.91, 116.32 39.92)"

# 执行分析
analysis_id, summary = analyze_trajectory_road_elements(
    trajectory_id=trajectory_id,
    trajectory_geom=trajectory_wkt
)

print(f"分析ID: {analysis_id}")
print(f"分析汇总: {summary}")
```

### 2. 自定义配置

```python
from spdatalab.fusion.trajectory_road_analysis import (
    TrajectoryRoadAnalysisConfig,
    TrajectoryRoadAnalyzer
)

# 自定义配置
config = TrajectoryRoadAnalysisConfig(
    buffer_distance=5.0,  # 修改缓冲区距离为5m
    forward_chain_limit=800.0,  # 修改前向链路限制为800m
    backward_chain_limit=200.0,  # 修改后向链路限制为200m
)

# 创建分析器
analyzer = TrajectoryRoadAnalyzer(config)

# 执行分析
analysis_id = analyzer.analyze_trajectory_roads(
    trajectory_id="custom_trajectory",
    trajectory_geom="LINESTRING(...)"
)
```

### 3. 批量分析

```python
from spdatalab.fusion.trajectory_road_analysis import analyze_trajectory_from_table

# 批量分析轨迹表中的数据
results = analyze_trajectory_from_table(
    trajectory_table="my_trajectories",
    trajectory_id_column="scene_id",
    trajectory_geom_column="geometry",
    limit=10  # 限制分析前10条记录
)

# 处理结果
for trajectory_id, analysis_id, summary in results:
    if analysis_id:
        print(f"轨迹 {trajectory_id} 分析完成: {analysis_id}")
    else:
        print(f"轨迹 {trajectory_id} 分析失败: {summary.get('error')}")
```

### 4. 生成分析报告

```python
from spdatalab.fusion.trajectory_road_analysis import create_trajectory_road_analysis_report

# 生成Markdown格式的分析报告
report = create_trajectory_road_analysis_report(analysis_id)
print(report)

# 保存报告到文件
with open(f"analysis_report_{analysis_id}.md", "w", encoding="utf-8") as f:
    f.write(report)
```

### 5. QGIS可视化

```python
from spdatalab.fusion.trajectory_road_analysis import export_trajectory_road_results_for_qgis

# 导出QGIS视图
export_info = export_trajectory_road_results_for_qgis(analysis_id)

print("QGIS视图:")
for view_type, view_name in export_info.items():
    print(f"- {view_type}: {view_name}")
```

## 数据结构

### 输入数据
- **轨迹ID**：唯一标识轨迹的字符串
- **轨迹几何**：WKT格式的LINESTRING几何字符串

### 输出数据结构

#### 分析汇总信息
```python
{
    "analysis_id": "trajectory_road_20231201_143022",
    "direct_intersect": 15,      # 直接相交的lane数量
    "intersection_related": 8,    # 路口相关的lane数量  
    "road_related": 25,          # 道路相关的lane数量
    "chain_forward": 12,         # 前向链路lane数量
    "chain_backward": 6,         # 后向链路lane数量
    "intersection_count": 3,     # 相交的intersection数量
    "road_count": 8,            # 涉及的road数量
    "analysis_time": "2023-12-01T14:30:22"
}
```

#### 数据库表结构

##### 主分析表 (trajectory_road_analysis)
```sql
CREATE TABLE trajectory_road_analysis (
    id SERIAL PRIMARY KEY,
    analysis_id VARCHAR(100) NOT NULL,
    trajectory_id VARCHAR(100) NOT NULL,
    original_trajectory_geom GEOMETRY,
    buffer_trajectory_geom GEOMETRY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

##### Lane结果表 (trajectory_road_lanes)
```sql
CREATE TABLE trajectory_road_lanes (
    id SERIAL PRIMARY KEY,
    analysis_id VARCHAR(100) NOT NULL,
    lane_id BIGINT NOT NULL,
    lane_type VARCHAR(50),  -- 'direct_intersect', 'intersection_related', 'road_related', 'chain_forward', 'chain_backward'
    road_id BIGINT,
    distance_from_trajectory FLOAT,
    chain_depth INTEGER,
    geometry GEOMETRY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

##### Intersection结果表 (trajectory_road_intersections)
```sql
CREATE TABLE trajectory_road_intersections (
    id SERIAL PRIMARY KEY,
    analysis_id VARCHAR(100) NOT NULL,
    intersection_id BIGINT NOT NULL,
    intersection_type INTEGER,
    intersection_subtype INTEGER,
    geometry GEOMETRY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

##### Road结果表 (trajectory_road_roads)
```sql
CREATE TABLE trajectory_road_roads (
    id SERIAL PRIMARY KEY,
    analysis_id VARCHAR(100) NOT NULL,
    road_id BIGINT NOT NULL,
    lane_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## 配置参数

### TrajectoryRoadAnalysisConfig

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `local_dsn` | str | "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres" | 本地数据库连接字符串 |
| `remote_dsn` | str | "postgresql+psycopg://**:**@10.170.30.193:9001/rcdatalake_gy1" | 远程数据库连接字符串 |
| `buffer_distance` | float | 3.0 | 轨迹膨胀距离(米) |
| `forward_chain_limit` | float | 500.0 | 前向链路扩展限制(米) |
| `backward_chain_limit` | float | 100.0 | 后向链路扩展限制(米) |
| `max_recursion_depth` | int | 50 | 最大递归深度 |
| `max_lanes_per_query` | int | 1000 | 单次查询最大lane数量 |
| `max_intersections_per_query` | int | 100 | 单次查询最大intersection数量 |
| `max_forward_chains` | int | 500 | 前向链路最大数量 |
| `max_backward_chains` | int | 200 | 后向链路最大数量 |
| `query_timeout` | int | 60 | 查询超时时间(秒) |
| `recursive_query_timeout` | int | 120 | 递归查询超时时间(秒) |
| `lane_table` | str | "full_lane" | 远程lane表名 |
| `intersection_table` | str | "full_intersection" | 远程intersection表名 |
| `lanenextlane_table` | str | "full_lanenextlane" | 远程lanenextlane表名 |
| `road_table` | str | "full_road" | 远程road表名 |

## 高级用法

### 1. 自定义分析流程

```python
from spdatalab.fusion.trajectory_road_analysis import TrajectoryRoadAnalyzer

analyzer = TrajectoryRoadAnalyzer()

# 步骤1：创建缓冲区
buffer_geom = analyzer._create_trajectory_buffer(trajectory_wkt)

# 步骤2：查找相交lane
intersecting_lanes = analyzer._find_intersecting_lanes(analysis_id, buffer_geom)

# 步骤3：查找相交intersection
intersecting_intersections = analyzer._find_intersecting_intersections(analysis_id, buffer_geom)

# 步骤4：扩展相关数据
# ... 其他步骤
```

### 2. 结合trajectory模块使用

```python
from spdatalab.dataset.trajectory import process_scene_mappings
from spdatalab.fusion.trajectory_road_analysis import analyze_trajectory_from_table

# 1. 先用trajectory模块生成轨迹数据
mappings_df = load_scene_data_mappings("scenes.txt")
stats = process_scene_mappings(mappings_df, "my_trajectories")

# 2. 再用trajectory_road_analysis分析道路元素
results = analyze_trajectory_from_table(
    trajectory_table="my_trajectories",
    trajectory_id_column="scene_id",
    trajectory_geom_column="geometry"
)
```

## 故障排除

### 常见问题

1. **数据库连接失败**
   - 检查数据库连接字符串是否正确
   - 确认数据库服务是否运行
   - 验证网络连接
   - 检查连接池配置是否合适

2. **空间查询失败**
   - 检查轨迹几何是否为有效的WKT格式
   - 确认数据库中的几何数据是否正确
   - 验证空间索引是否存在
   - 确认查询结果没有超出限制

3. **缓冲区创建失败**
   - 检查轨迹几何是否为LINESTRING类型
   - 确认坐标系统设置正确（默认4326）
   - 验证输入几何数据的完整性

4. **递归查询超时**
   - 减小chain_limit参数
   - 降低max_recursion_depth参数（建议≤10）
   - 检查lanenextlane表的数据质量
   - 增加recursive_query_timeout设置
   - 考虑分批处理大量数据

5. **查询结果过多**
   - 调整max_lanes_per_query等限制参数
   - 优化轨迹缓冲区大小
   - 考虑空间索引优化

6. **SQL格式错误**
   - 确保所有SQL查询都使用text()包装
   - 使用参数化查询避免SQL注入
   - 检查表名和字段名是否正确

### 调试技巧

1. **启用详细日志**
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

2. **检查中间结果**
```python
# 检查缓冲区几何
buffer_geom = analyzer._create_trajectory_buffer(trajectory_wkt)
print(f"缓冲区几何: {buffer_geom}")

# 检查相交lane
intersecting_lanes = analyzer._find_intersecting_lanes(analysis_id, buffer_geom)
print(f"相交lane数量: {len(intersecting_lanes)}")
```

3. **使用测试脚本**
```bash
# 运行完整测试（包含远程数据库查询）
python test_trajectory_road_analysis.py

# 运行简化测试（推荐，避免远程数据库超时）
python test_trajectory_road_analysis_simple.py
```

## 性能优化

### 1. 数据库优化
- 确保几何列上有空间索引
- 适当调整数据库连接池大小
- 使用合适的几何数据类型

### 2. 查询优化
- 合理设置缓冲区距离
- 限制递归深度
- 批量处理数据

### 3. 内存管理
- 分批处理大量轨迹
- 及时清理中间结果
- 监控内存使用情况

## 扩展开发

### 1. 添加新的分析类型
```python
class CustomTrajectoryRoadAnalyzer(TrajectoryRoadAnalyzer):
    def _custom_analysis(self, analysis_id: str, lanes_df: pd.DataFrame):
        # 自定义分析逻辑
        pass
```

### 2. 自定义导出格式
```python
def export_to_custom_format(analysis_id: str) -> dict:
    # 自定义导出逻辑
    pass
```

## 相关模块

- **trajectory**：轨迹生成模块
- **toll_station_analysis**：收费站分析模块
- **spatial_join_production**：空间连接模块

## 版本历史

- **v1.0.0**: 初始版本，支持基本的轨迹道路分析功能 