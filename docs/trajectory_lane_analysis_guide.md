# 轨迹车道分析模块使用指南

## 概述

轨迹车道分析模块是一个基于空间分析的轨迹处理工具，用于分析轨迹与车道的空间关系。该模块可以将轨迹按照邻近车道进行分段、采样和缓冲区分析，识别轨迹在不同车道上的行驶特征。

## 功能特性

### 核心功能
- **轨迹polyline生成**: 将轨迹点按timestamp排序生成polyline格式
- **多种采样策略**: 距离采样、时间采样、均匀采样
- **滑窗车道分析**: 自动识别车道变化，生成轨迹分段
- **缓冲区分析**: 为车道创建缓冲区，过滤轨迹点
- **轨迹质量检查**: 智能过滤，避免噪声数据
- **轨迹重构**: 重新连接完整轨迹线
- **轨迹简化**: Douglas-Peucker算法减少数据量

### 数据处理流程

```
scene_id → data_name → trajectory_points → polyline → 采样 → 滑窗分析 → 车道分段 → buffer分析 → 轨迹质量检查 → 轨迹重构 → 结果输出
```

## 安装要求

### 系统依赖
- Python 3.8+
- PostgreSQL + PostGIS
- 车道数据表（包含几何信息）

### Python依赖
```bash
pip install geopandas pandas numpy shapely sqlalchemy psycopg2-binary
```

## 配置说明

### 基础配置
```python
config = {
    # 输入配置
    'input_format': 'scene_id_list',
    'polyline_output': True,
    
    # 采样配置
    'sampling_strategy': 'distance',  # 'distance', 'time', 'uniform'
    'distance_interval': 10.0,        # 距离采样间隔（米）
    'time_interval': 5.0,             # 时间采样间隔（秒）
    'uniform_step': 50,               # 均匀采样步长
    
    # 滑窗配置
    'window_size': 20,                # 滑窗大小（采样点数）
    'window_overlap': 0.5,            # 滑窗重叠率
    
    # 车道分析配置
    'lane_table': 'public.road_lanes',
    'buffer_radius': 15.0,            # 缓冲区半径（米）
    'max_lane_distance': 50.0,        # 最大车道搜索距离（米）
    
    # 轨迹质量检查配置
    'min_points_single_lane': 5,      # 单车道最少点数
    'enable_multi_lane_filter': True, # 启用多车道过滤
    
    # 简化配置
    'simplify_tolerance': 2.0,        # 几何简化容差（米）
    'enable_simplification': True,
    
    # 性能配置
    'batch_size': 100,
    'max_workers': 4
}
```

### 质量检查规则
- **多车道轨迹**: 自动保留（无论点数）
- **单车道轨迹**: 需要 ≥ `min_points_single_lane` 个点才保留
- **无车道覆盖**: 自动丢弃

## 使用方法

### 1. 命令行使用

#### 基础用法
```bash
# 使用scene_id列表文件
python -m spdatalab.fusion.trajectory_lane_analysis \
    --input scenes.txt \
    --output-prefix my_analysis

# 使用dataset文件
python -m spdatalab.fusion.trajectory_lane_analysis \
    --input dataset.json \
    --output-prefix my_analysis
```

#### 高级参数
```bash
python -m spdatalab.fusion.trajectory_lane_analysis \
    --input scenes.txt \
    --output-prefix my_analysis \
    --sampling-strategy distance \
    --distance-interval 5.0 \
    --buffer-radius 20.0 \
    --min-points-single-lane 8 \
    --lane-table public.road_lanes \
    --batch-size 50 \
    --verbose
```

### 2. Python API使用

#### 基础使用
```python
from spdatalab.fusion.trajectory_lane_analysis import TrajectoryLaneAnalyzer
from spdatalab.dataset.trajectory import load_scene_data_mappings

# 创建分析器
config = {
    'buffer_radius': 15.0,
    'min_points_single_lane': 5,
    'sampling_strategy': 'distance'
}
analyzer = TrajectoryLaneAnalyzer(config)

# 加载数据
mappings_df = load_scene_data_mappings('input_file.txt')

# 执行分析
stats = analyzer.process_scene_mappings(mappings_df)

# 查看结果
print(f"成功分析 {stats['successful_trajectories']} 条轨迹")
print(f"质量检查通过率: {stats['quality_passed']/stats['total_scenes']:.2%}")
```

#### 高级用法
```python
# 自定义配置
custom_config = {
    'sampling_strategy': 'time',
    'time_interval': 3.0,
    'window_size': 15,
    'buffer_radius': 25.0,
    'lane_table': 'my_schema.my_lanes',
    'min_points_single_lane': 10,
    'simplify_tolerance': 1.0,
    'batch_size': 200
}

analyzer = TrajectoryLaneAnalyzer(custom_config)

# 单步处理示例
scene_id = "test_scene"
data_name = "test_data"

# 1. 获取轨迹点
points_df = fetch_trajectory_points(data_name)

# 2. 构建polyline
polyline_data = analyzer.build_trajectory_polyline(scene_id, data_name, points_df)

# 3. 采样
sampled_points = analyzer.sample_trajectory(polyline_data)

# 4. 滑窗分析
segments = analyzer.sliding_window_analysis(sampled_points)

# 5. 质量检查
buffer_results = []
for segment in segments:
    lane_id = segment['lane_id']
    buffer_geom = analyzer.create_lane_buffer(lane_id)
    filtered_points = analyzer.filter_trajectory_by_buffer(segment['points'], buffer_geom)
    buffer_results.append({
        'lane_id': lane_id,
        'points': filtered_points,
        'points_count': len(filtered_points)
    })

quality_result = analyzer.check_trajectory_quality(data_name, buffer_results)
print(f"质量检查结果: {quality_result['status']} - {quality_result['reason']}")
```

## 数据库表结构

### 输出表

#### 1. 轨迹质量检查表 (trajectory_quality_check)
```sql
CREATE TABLE trajectory_quality_check (
    id SERIAL PRIMARY KEY,
    scene_id TEXT NOT NULL,
    data_name TEXT NOT NULL,
    total_lanes_covered INTEGER,
    total_points_in_buffer INTEGER,
    quality_status TEXT NOT NULL,
    failure_reason TEXT,
    lanes_list TEXT[],
    avg_speed NUMERIC(8,2),
    max_speed NUMERIC(8,2),
    min_speed NUMERIC(8,2),
    avp_ratio NUMERIC(5,3),
    trajectory_length NUMERIC(10,2),
    reconstructed_trajectory GEOMETRY(LINESTRING, 4326),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### 2. 轨迹分段表 (trajectory_lane_segments)
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

#### 3. 缓冲区分析表 (trajectory_lane_buffer)
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

### 输入表要求

#### 车道数据表 (lane_table)
```sql
-- 最低要求的字段
CREATE TABLE public.road_lanes (
    id TEXT PRIMARY KEY,        -- 车道ID
    geometry GEOMETRY(LINESTRING, 4326),  -- 车道几何
    -- 其他可选字段...
);

-- 必要的空间索引
CREATE INDEX idx_road_lanes_geometry ON public.road_lanes USING GIST(geometry);
```

## 性能优化建议

### 1. 数据库优化
- 为车道表创建空间索引
- 适当设置PostGIS工作内存
- 定期更新表统计信息

### 2. 参数调优
- 根据数据密度调整采样间隔
- 根据内存容量调整批处理大小
- 根据车道密度调整缓冲区半径

### 3. 并行处理
```python
# 启用并行处理
config = {
    'enable_parallel': True,
    'max_workers': 4,
    'batch_size': 100
}
```

## 故障排除

### 常见问题

#### 1. 车道查询失败
**症状**: 大量"未找到最大距离内的车道"警告
**解决**: 
- 检查车道表是否存在几何数据
- 调整`max_lane_distance`参数
- 检查坐标系统是否匹配

#### 2. 内存不足
**症状**: 程序在处理大量轨迹时崩溃
**解决**: 
- 减少`batch_size`参数
- 增加系统内存
- 分批处理输入数据

#### 3. 处理速度慢
**症状**: 分析速度明显慢于预期
**解决**: 
- 检查空间索引是否存在
- 调整采样策略和间隔
- 启用并行处理

### 日志分析

#### 开启详细日志
```bash
python -m spdatalab.fusion.trajectory_lane_analysis \
    --input scenes.txt \
    --output-prefix my_analysis \
    --verbose
```

#### 关键日志信息
- `"滑窗分析完成，生成 X 个车道分段"`: 分段结果
- `"质量检查结果: passed/failed"`: 质量检查状态
- `"轨迹简化: X -> Y 点"`: 简化效果
- `"批量保存完成"`: 批处理进度

## 示例场景

### 场景1: 高速公路轨迹分析
```python
# 高速公路配置
highway_config = {
    'sampling_strategy': 'distance',
    'distance_interval': 20.0,    # 高速场景用更大间隔
    'buffer_radius': 30.0,        # 高速车道较宽
    'min_points_single_lane': 3,   # 高速场景点数要求较低
    'window_size': 10,             # 较小窗口适应高速变化
    'max_lane_distance': 100.0     # 高速场景搜索距离更大
}
```

### 场景2: 城市道路分析
```python
# 城市道路配置
urban_config = {
    'sampling_strategy': 'time',
    'time_interval': 2.0,         # 城市场景用时间采样
    'buffer_radius': 10.0,        # 城市车道较窄
    'min_points_single_lane': 8,   # 城市场景要求更多点
    'window_size': 25,             # 较大窗口适应复杂路况
    'max_lane_distance': 30.0      # 城市场景搜索距离较小
}
```

### 场景3: 高精度分析
```python
# 高精度配置
precision_config = {
    'sampling_strategy': 'uniform',
    'uniform_step': 20,            # 更密集的采样
    'buffer_radius': 5.0,          # 更小的缓冲区
    'min_points_single_lane': 15,  # 更严格的点数要求
    'simplify_tolerance': 0.5,     # 更小的简化容差
    'enable_simplification': False # 禁用简化保持精度
}
```

## 扩展开发

### 自定义采样策略
```python
class CustomTrajectoryLaneAnalyzer(TrajectoryLaneAnalyzer):
    def _custom_sampling(self, polyline_data: Dict) -> List[Dict]:
        """自定义采样策略"""
        # 实现自定义采样逻辑
        pass
    
    def sample_trajectory(self, polyline_data: Dict) -> List[Dict]:
        if self.config['sampling_strategy'] == 'custom':
            return self._custom_sampling(polyline_data)
        return super().sample_trajectory(polyline_data)
```

### 自定义质量检查
```python
def custom_quality_check(self, dataset_name: str, buffer_results: List[Dict]) -> Dict:
    """自定义质量检查逻辑"""
    # 实现自定义质量检查
    base_result = super().check_trajectory_quality(dataset_name, buffer_results)
    
    # 添加额外的质量检查规则
    if some_custom_condition:
        base_result['status'] = 'failed'
        base_result['reason'] = 'Custom quality check failed'
    
    return base_result
```

## 版本更新日志

### v1.0.0
- 初始版本发布
- 支持三种采样策略
- 实现轨迹质量检查
- 支持批量处理
- 完整的CLI接口

---

## 技术支持

如需技术支持，请参考：
1. 检查日志文件了解详细错误信息
2. 参考故障排除章节
3. 查看单元测试了解用法示例

---

*最后更新: 2024年* 