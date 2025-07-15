# 轨迹车道分析批量处理修复

## 修复概述

本次修复解决了轨迹车道分析模块在批量处理多个轨迹时遇到的两个关键问题：

1. **PostgreSQL几何列查询错误** - 修复了geometry_columns表查询中的列名错误
2. **批量分析表名冲突** - 修复了多轨迹分析时表名相同导致数据覆盖的问题

## 问题1：几何列查询错误 ✅

### 问题描述
```
ERROR - (psycopg.errors.UndefinedColumn) column "column_name" does not exist
LINE 2: SELECT column_name, coord_dimension 
FROM geometry_columns
```

### 原因分析
- 代码中查询PostgreSQL的`geometry_columns`表时使用了错误的列名
- PostgreSQL的`geometry_columns`表结构中应该使用`f_geometry_column`而不是`column_name`

### 修复方案
修改所有`geometry_columns`表查询，使用正确的列名：

```sql
-- 修复前（错误）
SELECT column_name, coord_dimension 
FROM geometry_columns 
WHERE f_table_schema = 'public' 
AND f_table_name = 'table_name'
AND f_geometry_column = 'geometry'

-- 修复后（正确）
SELECT f_geometry_column, coord_dimension 
FROM geometry_columns 
WHERE f_table_schema = 'public' 
AND f_table_name = 'table_name'
AND f_geometry_column = 'geometry'
```

### 修复位置
- `src/spdatalab/fusion/trajectory_lane_analysis.py:1261`
- `src/spdatalab/fusion/trajectory_lane_analysis.py:1570`

## 问题2：批量分析表名冲突 ✅

### 问题描述
- 批量分析多个轨迹时，只有最后一个轨迹的数据被保存
- 前面轨迹的数据被后续轨迹覆盖

### 原因分析
1. 批量分析时，所有轨迹使用相同的`road_analysis_id`
2. 表名生成逻辑基于`road_analysis_id`，导致相同的表名
3. 后续轨迹在保存时删除了前面轨迹的数据

### 修复方案
修改`_generate_dynamic_table_names`方法，为每个轨迹生成唯一的表名后缀：

```python
# 修复前：所有轨迹使用相同表名
table_names = {
    'lane_analysis_main_table': f"{base_name}_lanes",
    'lane_analysis_summary_table': f"{base_name}_lanes_summary",
    'lane_hits_table': f"{base_name}_lane_hits", 
    'lane_trajectories_table': f"{base_name}_lane_trajectories"
}

# 修复后：每个轨迹使用唯一表名
# 从analysis_id中提取trajectory_id作为后缀
trajectory_suffix = self._extract_trajectory_suffix(analysis_id)
table_names = {
    'lane_analysis_main_table': f"{base_name}_lanes{trajectory_suffix}",
    'lane_analysis_summary_table': f"{base_name}_lanes_summary{trajectory_suffix}",
    'lane_hits_table': f"{base_name}_lane_hits{trajectory_suffix}", 
    'lane_trajectories_table': f"{base_name}_lane_trajectories{trajectory_suffix}"
}
```

### 后缀生成逻辑
1. 从`analysis_id`中解析轨迹标识符
2. 使用轨迹ID的前8位作为后缀（避免表名过长）
3. 如果无法解析，使用`analysis_id`的MD5哈希值前8位

## 修复验证

### 测试用例
创建了`test_lane_analysis_fixes.py`测试脚本，包含：

1. **geometry_columns查询测试** - 验证查询不会报错
2. **表名唯一性测试** - 验证不同轨迹生成不同表名
3. **analysis_id解析测试** - 验证各种格式的analysis_id都能正确解析

### 预期结果
- ✅ 几何列查询不再报错
- ✅ 批量分析时每个轨迹生成独立的结果表
- ✅ 所有轨迹的数据都能正确保存，不会相互覆盖

## 表名示例

### 修复前（冲突）
```
轨迹1: integrated_20250715_073547_lanes
轨迹2: integrated_20250715_073547_lanes          # 与轨迹1相同！
轨迹3: integrated_20250715_073547_lanes          # 与轨迹1、2相同！
```

### 修复后（唯一）
```
轨迹1: integrated_20250715_073547_lanes_abc123de
轨迹2: integrated_20250715_073547_lanes_xyz789uv
轨迹3: integrated_20250715_073547_lanes_def456gh
```

## 影响评估

### 正面影响
- **数据完整性** - 所有轨迹的分析结果都能保存
- **系统稳定性** - 不再出现SQL查询错误
- **可追溯性** - 每个轨迹都有独立的结果表，便于查询和调试

### 注意事项
- **表数量增加** - 批量分析会创建更多表（每轨迹4个表）
- **表名长度** - 自动处理长表名，截断到PostgreSQL限制内
- **向后兼容** - 修复不影响现有单轨迹分析功能

## 使用建议

### 批量分析最佳实践
1. 合理控制批量大小（建议≤10个轨迹/批次）
2. 定期清理过时的分析结果表
3. 使用有意义的`analysis_id`便于后续识别

### 数据库维护
```sql
-- 查看某个批次的所有结果表
SELECT tablename 
FROM pg_tables 
WHERE tablename LIKE 'integrated_20250715_073547_%';

-- 清理过时的分析结果（示例）
DROP TABLE IF EXISTS integrated_20250715_073547_lanes_abc123de;
DROP TABLE IF EXISTS integrated_20250715_073547_lanes_summary_abc123de;
-- ... 等等
```

## 相关文件

### 修改的文件
- `src/spdatalab/fusion/trajectory_lane_analysis.py` - 主要修复文件

### 新增的文件
- `test_lane_analysis_fixes.py` - 修复验证测试
- `docs/trajectory_lane_analysis_batch_fixes.md` - 本文档

### 删除的文件
- `docs/trajectory_lane_analysis_guide.md` - 过时的指南
- `docs/trajectory_lane_analysis_fixes.md` - 早期修复文档
- `docs/trajectory_lane_analysis_direction_matching.md` - 功能说明文档

## 总结

这两个修复彻底解决了批量轨迹车道分析的关键问题，确保了：
- **SQL兼容性** - 所有数据库查询都能正确执行
- **数据完整性** - 每个轨迹的分析结果都能完整保存
- **系统可用性** - 批量分析功能稳定可靠

建议在生产环境使用前进行充分测试，验证修复效果。 