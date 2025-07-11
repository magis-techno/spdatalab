# 轨迹与车道空间关系分析方案

## 项目概述

本项目旨在基于现有的轨迹生成模块(`trajectory.py`)，实现轨迹与车道的空间关系分析。通过对轨迹进行分段、采样和缓冲区分析，识别轨迹在不同车道上的行驶特征。

## 技术方案

### 1. 整体架构

```
scene_id → data_name → trajectory_points → polyline → 采样 → 滑窗分析 → 车道分段 → buffer分析 → 轨迹质量检查 → 轨迹重构 → 结果输出
```

### 2. 核心功能模块

#### 2.1 输入数据处理
- **复用trajectory.py逻辑**: 支持scene_id输入，自动查询data_name
- **Polyline构建**: 将轨迹点按timestamp排序，构建polyline格式
- **数据源**: 继续使用`public.ddi_data_points`表

#### 2.2 轨迹采样策略
- **距离采样**: 按固定距离间隔采样（如每10米）
- **时间采样**: 按固定时间间隔采样（如每5秒）
- **均匀采样**: 按点数间隔采样（如每50个点）

#### 2.3 滑窗车道分析
- **滑窗遍历**: 使用重叠窗口遍历采样点
- **最近车道查询**: 基于PostGIS空间索引查找最近车道
- **轨迹分段**: 根据车道变化自动分段

#### 2.4 缓冲区分析
- **车道Buffer**: 为每个车道创建缓冲区（可配置半径）
- **轨迹过滤**: 过滤出buffer内的轨迹点
- **轨迹质量检查**: 检查dataset_name的轨迹点数量和车道覆盖情况
- **轨迹连线**: 将通过质量检查的轨迹点按dataset_name和timestamp连成线
- **几何简化**: 应用Douglas-Peucker等算法减少数据量

### 3. 数据结构设计

#### 3.1 轨迹-车道关联表
```sql
CREATE TABLE trajectory_lane_segments (
    id SERIAL PRIMARY KEY,
    scene_id TEXT NOT NULL,
    data_name TEXT NOT NULL,
    lane_id TEXT NOT NULL,
    segment_index INTEGER,
    start_time BIGINT,
    end_time BIGINT,
    start_point_index INTEGER,
    end_point_index INTEGER,
    avg_speed NUMERIC(8,2),
    max_speed NUMERIC(8,2),
    min_speed NUMERIC(8,2),
    segment_length NUMERIC(10,2),
    geometry GEOMETRY(LINESTRING, 4326),
    original_points_count INTEGER,
    simplified_points_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### 3.2 缓冲区分析结果表
```sql
CREATE TABLE trajectory_lane_buffer (
    id SERIAL PRIMARY KEY,
    scene_id TEXT NOT NULL,
    data_name TEXT NOT NULL,
    lane_id TEXT NOT NULL,
    buffer_radius NUMERIC(6,2),
    filtered_trajectory GEOMETRY(LINESTRING, 4326),
    points_in_buffer INTEGER,
    total_points INTEGER,
    coverage_ratio NUMERIC(5,3),
    trajectory_length NUMERIC(10,2),
    avg_distance_to_lane NUMERIC(8,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### 3.3 轨迹质量检查结果表
```sql
CREATE TABLE trajectory_quality_check (
    id SERIAL PRIMARY KEY,
    scene_id TEXT NOT NULL,
    data_name TEXT NOT NULL,
    total_lanes_covered INTEGER,
    total_points_in_buffer INTEGER,
    quality_status TEXT NOT NULL, -- 'passed', 'failed_single_lane', 'failed_insufficient_points'
    failure_reason TEXT,
    lanes_list TEXT[], -- 涉及的车道列表
    reconstructed_trajectory GEOMETRY(LINESTRING, 4326),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 4. 配置参数

```python
CONFIG = {
    # 输入配置
    'input_format': 'scene_id_list',
    'polyline_output': True,
    
    # 采样配置
    'sampling_strategy': 'distance',  # 'distance', 'time', 'uniform'
    'distance_interval': 10.0,        # 米
    'time_interval': 5.0,             # 秒
    'uniform_step': 50,               # 点数
    
    # 滑窗配置
    'window_size': 20,                # 采样点数
    'window_overlap': 0.5,            # 重叠率
    
    # 车道分析配置
    'lane_table': 'public.road_lanes',
    'buffer_radius': 15.0,            # 米
    'max_lane_distance': 50.0,        # 米
    
    # 轨迹质量检查配置
    'min_points_single_lane': 5,      # 单车道最少点数
    'enable_multi_lane_filter': True, # 启用多车道过滤
    
    # 简化配置
    'simplify_tolerance': 2.0,        # 米
    'enable_simplification': True,
    
    # 性能配置
    'batch_size': 100,
    'enable_parallel': True,
    'max_workers': 4
}
```

### 5. 处理流程

#### 5.1 预处理阶段
1. 验证输入参数和配置
2. 检查车道数据表是否存在
3. 创建结果表和索引
4. 初始化空间参考系统

#### 5.2 主处理阶段
1. **加载scene_id映射**: 复用trajectory.py的逻辑
2. **查询轨迹数据**: 从ddi_data_points表查询
3. **构建polyline**: 按timestamp排序生成坐标序列
4. **轨迹采样**: 应用选定的采样策略
5. **滑窗分析**: 识别车道变化，生成分段
6. **缓冲区分析**: 为每个车道创建buffer并过滤轨迹
7. **轨迹质量检查**: 检查dataset_name的轨迹点数量和车道覆盖情况
8. **轨迹连线重构**: 将通过质量检查的轨迹点按dataset_name连成完整轨迹
9. **几何简化**: 减少输出数据量
10. **结果存储**: 批量插入到结果表

#### 5.3 后处理阶段
1. 生成分析统计报告
2. 创建空间索引
3. 数据质量检查
4. 输出处理日志

## 实现计划 (TODO List)

### 阶段1: 基础框架 (1-2天)
- [ ] **setup_lane_analysis_module** - 创建轨迹车道分析模块结构
  - 新建`trajectory_lane_analysis.py`
  - 设置基础框架和配置
  - 定义核心类和接口

### 阶段2: 核心功能 (3-4天)
- [ ] **implement_polyline_builder** - 实现polyline构建器
  - 复用trajectory.py的输入逻辑
  - 构建polyline格式轨迹数据
  - 支持多种输入格式

- [ ] **implement_sampling_strategies** - 实现轨迹采样策略
  - 距离采样算法
  - 时间采样算法
  - 均匀采样算法

- [ ] **implement_sliding_window** - 实现滑窗分析算法
  - 滑窗遍历采样点
  - 检测车道变化
  - 生成轨迹分段

- [ ] **implement_lane_query** - 实现车道查询功能
  - 基于PostGIS空间索引
  - 最近车道查找
  - 距离计算优化

### 阶段3: 空间分析 (3-4天)
- [ ] **implement_buffer_analysis** - 实现车道buffer分析
  - 创建车道缓冲区
  - 过滤轨迹点
  - 计算覆盖率统计

- [ ] **implement_trajectory_quality_check** - 实现轨迹质量检查
  - 检查dataset_name的车道覆盖情况
  - 应用单车道最少点数过滤
  - 生成质量检查报告

- [ ] **implement_trajectory_reconstruction** - 实现轨迹重构
  - 将通过质量检查的轨迹点按dataset_name连线
  - 按timestamp排序确保时间连续性
  - 生成完整轨迹几何

- [ ] **implement_trajectory_simplification** - 实现轨迹简化算法
  - Douglas-Peucker算法
  - 距离阈值简化
  - 时间间隔简化

### 阶段4: 数据存储 (1-2天)
- [ ] **create_database_schema** - 创建数据库表结构
  - trajectory_lane_segments表
  - trajectory_lane_buffer表
  - trajectory_quality_check表
  - 创建空间索引和普通索引

### 阶段5: 系统集成 (2-3天)
- [ ] **implement_batch_processing** - 实现批量处理逻辑
  - 支持多个scene_id
  - 并行处理
  - 批量数据插入

- [ ] **implement_cli_interface** - 实现命令行接口
  - 参考trajectory.py的CLI设计
  - 支持参数配置
  - 添加帮助文档

### 阶段6: 质量保证 (2-3天)
- [ ] **add_error_handling** - 添加错误处理和日志
  - 完善异常处理
  - 添加详细的处理日志
  - 优雅退出机制

- [ ] **create_unit_tests** - 创建单元测试
  - 测试各个功能模块
  - 验证数据正确性
  - 性能测试

### 阶段7: 优化和文档 (1-2天)
- [ ] **performance_optimization** - 性能优化
  - 优化空间查询
  - 调整批处理大小
  - 添加并行处理

- [ ] **create_documentation** - 创建使用文档
  - 功能说明
  - 参数配置
  - 使用示例

## 核心算法详解

### 轨迹质量检查算法

```python
def check_trajectory_quality(dataset_name: str, buffer_results: List[Dict]) -> Dict:
    """
    检查轨迹质量，决定是否保留该dataset_name的轨迹
    
    参数:
        dataset_name: 数据集名称
        buffer_results: 该dataset_name在各车道buffer中的轨迹点
    
    返回:
        质量检查结果
    """
    # 1. 统计涉及的车道数量
    lanes_covered = set([result['lane_id'] for result in buffer_results])
    total_lanes = len(lanes_covered)
    
    # 2. 统计总轨迹点数
    total_points = sum([result['points_count'] for result in buffer_results])
    
    # 3. 应用过滤规则
    if total_lanes > 1:
        # 多车道情况：保留（无论点数多少）
        status = 'passed'
        reason = f'多车道轨迹，涉及{total_lanes}个车道'
    elif total_lanes == 1 and total_points >= config['min_points_single_lane']:
        # 单车道但点数足够：保留
        status = 'passed'
        reason = f'单车道轨迹但点数充足({total_points}点)'
    else:
        # 单车道且点数不足：丢弃
        status = 'failed'
        reason = f'单车道轨迹点数不足({total_points}点 < {config["min_points_single_lane"]}点)'
    
    return {
        'dataset_name': dataset_name,
        'status': status,
        'reason': reason,
        'lanes_covered': list(lanes_covered),
        'total_points': total_points,
        'total_lanes': total_lanes
    }
```

### 轨迹重构算法

```python
def reconstruct_trajectory(dataset_name: str, quality_check_result: Dict) -> Dict:
    """
    重构完整轨迹，将通过质量检查的轨迹点连成线
    
    参数:
        dataset_name: 数据集名称
        quality_check_result: 质量检查结果
    
    返回:
        重构后的轨迹
    """
    if quality_check_result['status'] != 'passed':
        return None
    
    # 1. 查询该dataset_name的所有原始轨迹点
    original_points = fetch_trajectory_points(dataset_name)
    
    # 2. 按timestamp排序
    original_points = original_points.sort_values('timestamp')
    
    # 3. 构建完整轨迹线
    coordinates = [(row['longitude'], row['latitude']) for _, row in original_points.iterrows()]
    trajectory_geom = LineString(coordinates)
    
    # 4. 计算轨迹统计
    return {
        'dataset_name': dataset_name,
        'geometry': trajectory_geom,
        'start_time': original_points['timestamp'].min(),
        'end_time': original_points['timestamp'].max(),
        'total_points': len(original_points),
        'lanes_covered': quality_check_result['lanes_covered'],
        'avg_speed': original_points['twist_linear'].mean(),
        'trajectory_length': trajectory_geom.length
    }
```

## 预期成果

1. **新模块文件**: `src/spdatalab/fusion/trajectory_lane_analysis.py`
2. **数据库表**: 三个新的分析结果表
   - `trajectory_lane_segments`: 轨迹-车道分段表
   - `trajectory_lane_buffer`: 缓冲区分析结果表
   - `trajectory_quality_check`: 轨迹质量检查结果表
3. **CLI工具**: 支持命令行调用的分析工具
4. **核心功能**: 
   - 轨迹质量检查（多车道保留，单车道≥5点）
   - 轨迹重构（连成完整轨迹线）
   - 智能过滤（避免噪声数据）
5. **测试用例**: 完整的单元测试套件
6. **使用文档**: 详细的使用说明和示例

## 技术依赖

- **已有模块**: trajectory.py的输入处理逻辑
- **数据库**: PostgreSQL + PostGIS
- **Python库**: geopandas, shapely, sqlalchemy
- **车道数据**: 需要准备车道几何数据表

## 风险评估

1. **数据质量**: 车道数据的完整性和准确性
2. **性能问题**: 大量轨迹数据的处理效率
3. **空间计算**: 复杂几何运算的精度
4. **内存使用**: 大数据集的内存管理
5. **轨迹重构**: 质量检查可能过滤掉有用数据
6. **参数调优**: 单车道最少点数阈值需要实际测试调整

## 后续扩展

1. 支持多种车道数据格式
2. 添加轨迹质量评估功能
3. 集成QGIS可视化
4. 支持实时流处理
5. 动态调整质量检查阈值
6. 添加轨迹异常检测
7. 支持多种轨迹重构策略
8. 车道变化事件检测 