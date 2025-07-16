# 新增字段功能说明

## 概述

在现有的Polygon轨迹查询功能基础上，新增了三个重要字段：

1. **scene_id** (text) - 场景ID，通过data_name反查数据库获得
2. **event_id** (integer) - 事件ID，自动递增的整数
3. **event_name** (varchar765) - 事件名称，基于event_id和dataset_name生成

## 功能特点

### 🔍 Scene ID 反查
- **查询逻辑**: 使用 `data_name` 反查 `transform.ods_t_data_fragment_datalake` 表获取对应的 `scene_id`
- **SQL**: `SELECT origin_name AS data_name, id AS scene_id FROM transform.ods_t_data_fragment_datalake WHERE origin_name IN (...)`
- **容错处理**: 如果查不到对应的scene_id，字段值将为空字符串

### 🔢 Event ID 自动生成
- **生成规则**: 从1开始自动递增的整数
- **作用域**: 每次查询会话内唯一
- **用途**: 作为轨迹事件的唯一标识符

### 📝 Event Name 智能命名
- **命名格式**: `trajectory_{event_id}_{dataset_name}`
- **示例**: `trajectory_1_sample_dataset_001`
- **长度限制**: 最大765字符 (varchar765)

## 数据库表结构更新

新创建的轨迹表将包含以下字段：

```sql
CREATE TABLE trajectory_table (
    id serial PRIMARY KEY,
    dataset_name text NOT NULL,
    scene_id text,                    -- 新增：场景ID
    event_id integer,                 -- 新增：事件ID  
    event_name varchar(765),          -- 新增：事件名称
    start_time bigint,
    end_time bigint,
    duration bigint,
    point_count integer,
    avg_speed numeric(8,2),
    max_speed numeric(8,2),
    min_speed numeric(8,2),
    std_speed numeric(8,2),
    avp_ratio numeric(5,3),
    polygon_ids text[],
    geometry geometry(LINESTRING, 4326),
    created_at timestamp DEFAULT CURRENT_TIMESTAMP
);
```

### 索引优化

为新字段创建了相应的索引以提升查询性能：

```sql
CREATE INDEX idx_table_scene_id ON table_name(scene_id);
CREATE INDEX idx_table_event_id ON table_name(event_id);
CREATE INDEX idx_table_event_name ON table_name(event_name);
```

## 使用方法

### 1. 基础使用

```python
from src.spdatalab.dataset.polygon_trajectory_query import (
    HighPerformancePolygonTrajectoryQuery, 
    PolygonTrajectoryConfig
)

# 创建配置（默认启用新字段功能）
config = PolygonTrajectoryConfig()
query_processor = HighPerformancePolygonTrajectoryQuery(config)

# 执行查询
trajectories, stats = query_processor.process_complete_workflow(
    polygon_geojson="polygons.geojson",
    output_table="my_trajectories"
)

# 查看轨迹数据
for traj in trajectories:
    print(f"Dataset: {traj['dataset_name']}")
    print(f"Scene ID: {traj['scene_id']}")           # 新字段
    print(f"Event ID: {traj['event_id']}")           # 新字段
    print(f"Event Name: {traj['event_name']}")       # 新字段
    print(f"Points: {traj['point_count']}")
```

### 2. 数据库查询示例

保存到数据库后，可以使用SQL查询新字段：

```sql
-- 按scene_id查询轨迹
SELECT * FROM my_trajectories WHERE scene_id = 'specific_scene_id';

-- 按event_id范围查询
SELECT * FROM my_trajectories WHERE event_id BETWEEN 1 AND 10;

-- 查看event_name模式
SELECT event_name, COUNT(*) FROM my_trajectories GROUP BY event_name;

-- 统计scene_id覆盖率
SELECT 
    COUNT(*) as total_trajectories,
    COUNT(CASE WHEN scene_id != '' THEN 1 END) as with_scene_id,
    ROUND(COUNT(CASE WHEN scene_id != '' THEN 1 END) * 100.0 / COUNT(*), 2) as coverage_percentage
FROM my_trajectories;
```

## 性能影响

### 查询性能
- **新增查询**: Scene ID反查增加约0.1-0.5秒查询时间
- **批量优化**: 使用IN查询批量获取scene_id，避免逐个查询
- **缓存机制**: 同一查询会话内复用scene_id映射结果

### 存储开销
- **scene_id**: 约20-40字节/记录（根据ID长度）
- **event_id**: 4字节/记录
- **event_name**: 50-100字节/记录（根据dataset_name长度）
- **总计**: 约74-144字节额外存储/记录

## 兼容性

### 向后兼容
- ✅ 不影响现有查询功能
- ✅ 现有配置参数保持有效
- ✅ 输出格式向后兼容（增加字段）

### 数据库兼容
- ✅ 支持PostgreSQL 12+
- ✅ 支持PostGIS 3.0+
- ✅ 自动检测表结构，增量更新

## 测试验证

### 运行功能测试
```bash
# 基础功能测试
python test_new_fields.py

# 新字段演示
python examples/new_fields_example.py
```

### 预期输出示例
```
=== 轨迹 1 ===
📛 dataset_name: sample_dataset_001
🏷️ scene_id: abc123xyz789 
🔢 event_id: 1
📝 event_name: trajectory_1_sample_dataset_001
📍 点数: 156
⏱️ 持续时间: 45秒
```

## 故障排除

### Scene ID 查询失败
- **症状**: scene_id字段为空
- **原因**: data_name在源表中不存在
- **解决**: 检查data_name格式和源表数据完整性

### Event ID 重复
- **症状**: 同一查询会话中event_id重复
- **原因**: 代码逻辑错误（理论上不应发生）
- **解决**: 检查event_id_counter递增逻辑

### Event Name 过长
- **症状**: event_name保存时截断
- **原因**: dataset_name过长导致总长度超过765字符
- **解决**: 考虑缩短dataset_name或调整命名格式

## 更新记录

- **2025-07-16**: 新增scene_id、event_id、event_name三个字段
- **2025-07-16**: 添加相应数据库索引优化
- **2025-07-16**: 创建测试用例和使用示例

---

�� 如有问题或建议，请联系开发团队。 