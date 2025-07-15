# 轨迹车道方向匹配功能

## 功能概述

轨迹车道分析模块现在支持基于航向角度的方向校验，可以有效过滤掉对向车道，提高地图匹配的准确性。

## 主要特性

### 1. 智能航向计算
- 从轨迹坐标自动计算行驶航向
- 支持多种航向计算方法
- 考虑轨迹分段长度，确保航向计算的可靠性

### 2. 车道方向匹配
- 计算候选车道的几何航向
- 比较轨迹航向与车道航向
- 过滤掉角度差超过阈值的对向车道

### 3. 灵活配置
- 可调节的航向角度差阈值
- 可选择的航向计算方法
- 支持启用/禁用方向匹配

## 配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `enable_direction_matching` | `True` | 是否启用方向匹配 |
| `max_heading_difference` | `45.0` | 最大航向角度差（度） |
| `min_segment_length` | `10.0` | 最小分段长度（米） |
| `heading_calculation_method` | `'start_end'` | 航向计算方法 |

## 航向计算方法

### 1. start_end（推荐）
使用轨迹起点和终点计算整体航向，适合大部分场景。

### 2. weighted_average
根据每个分段的长度进行加权平均，适合复杂轨迹。

## 使用示例

### 命令行使用

```bash
# 启用方向匹配（默认）
python -m spdatalab.fusion.trajectory_lane_analysis \
  --trajectory-id my_traj \
  --trajectory-geom "LINESTRING(...)" \
  --road-analysis-id road_analysis_20241201_123456 \
  --enable-direction-matching \
  --max-heading-difference 45.0

# 禁用方向匹配
python -m spdatalab.fusion.trajectory_lane_analysis \
  --trajectory-id my_traj \
  --trajectory-geom "LINESTRING(...)" \
  --road-analysis-id road_analysis_20241201_123456 \
  --disable-direction-matching

# 自定义航向参数
python -m spdatalab.fusion.trajectory_lane_analysis \
  --trajectory-id my_traj \
  --trajectory-geom "LINESTRING(...)" \
  --road-analysis-id road_analysis_20241201_123456 \
  --max-heading-difference 30.0 \
  --min-segment-length 15.0 \
  --heading-method weighted_average
```

### 编程使用

```python
from spdatalab.fusion.trajectory_lane_analysis import TrajectoryLaneAnalyzer

# 配置方向匹配参数
config = {
    'enable_direction_matching': True,
    'max_heading_difference': 45.0,
    'min_segment_length': 10.0,
    'heading_calculation_method': 'start_end'
}

# 创建分析器
analyzer = TrajectoryLaneAnalyzer(config=config, road_analysis_id=road_analysis_id)

# 执行分析
result = analyzer.analyze_trajectory_neighbors(trajectory_id, trajectory_geom)
```

## 输出信息

### 基本统计
```
方向匹配统计:
  - 总候选车道: 15 个
  - 对向车道过滤: 7 个  
  - 最终同向车道: 8 个
  - 无航向信息车道: 0 个
```

### 详细信息
```
=== 方向匹配详情 ===
Lane 12345: 同向匹配
  - 轨迹航向: 85.3°
  - 车道航向: 92.1°
  - 航向差值: 6.8°
  - 距离: 0.000123

Lane 12346: 同向匹配
  - 轨迹航向: 85.3°
  - 车道航向: 78.9°
  - 航向差值: 6.4°
  - 距离: 0.000156
```

## 数据库存储

方向匹配信息会保存到车道分析结果表中：

```sql
-- 新增字段
trajectory_heading FLOAT,      -- 轨迹航向角度
lane_heading FLOAT,           -- 车道航向角度  
heading_difference FLOAT,     -- 航向角度差值
direction_matched BOOLEAN     -- 是否方向匹配
```

## 性能影响

- 航向计算是轻量级操作，对性能影响很小
- 方向过滤可以减少后续处理的数据量
- 总体上提高了分析效率和准确性

## 使用建议

1. **城市道路**：建议使用默认的45度阈值
2. **高速公路**：可以降低到30度以获得更严格的匹配
3. **复杂路口**：可以提高到60度以避免过度过滤
4. **短轨迹**：降低 `min_segment_length` 到5米
5. **长轨迹**：使用 `weighted_average` 方法获得更准确的航向

## 故障排查

### 常见问题

1. **所有车道都被过滤**
   - 检查 `max_heading_difference` 是否过小
   - 验证轨迹方向是否正确
   - 增加 `min_segment_length` 确保航向计算可靠

2. **航向计算不准确**
   - 检查轨迹分段长度是否足够
   - 尝试不同的航向计算方法
   - 验证轨迹几何数据质量

3. **性能较慢**
   - 方向匹配本身很快，检查其他配置参数
   - 减少 `max_lane_distance` 降低候选车道数量

### 调试技巧

使用 `--verbose` 参数查看详细的方向匹配日志：

```bash
python -m spdatalab.fusion.trajectory_lane_analysis \
  --trajectory-id my_traj \
  --trajectory-geom "LINESTRING(...)" \
  --road-analysis-id road_analysis_20241201_123456 \
  --verbose
```

这将输出每个车道的航向计算和匹配结果。 