# Event ID 数据类型修复总结

## 🚨 问题描述
```
ERROR: invalid input syntax for type integer: "330012.0"
CONTEXT: COPY table, line 1, column event_id: "330012.0"
```

PostgreSQL期望整数类型的`event_id`，但收到了浮点数格式的字符串。

## 🔍 根本原因

1. **数据库返回**: 从Hive查询的`event_id`字段返回浮点数类型 `330012.0`
2. **pandas推断**: 创建GeoDataFrame时，包含NaN的列被推断为`float64`类型
3. **to_postgis转换**: GeoPandas将浮点数转换为字符串 `"330012.0"`
4. **PostgreSQL拒绝**: 数据库无法将浮点数字符串解析为整数

## ✅ 解决方案

### 1. 字典构建阶段的预处理
```python
# 在构建映射字典时确保类型正确
event_ids_cleaned = scene_id_mappings['event_id'].apply(
    lambda x: int(float(x)) if pd.notna(x) and x != '' else None
)
data_name_to_event_id = dict(zip(scene_id_mappings['data_name'], event_ids_cleaned))
```

### 2. GeoDataFrame保存前的强制转换
```python
# 处理pandas将整数转换为浮点数的问题
if 'event_id' in gdf.columns:
    valid_mask = gdf['event_id'].notna()
    new_event_ids = pd.Series([None] * len(gdf), dtype=object)
    
    if valid_mask.any():
        valid_values = gdf.loc[valid_mask, 'event_id']
        converted_values = valid_values.apply(lambda x: int(x))
        new_event_ids.loc[valid_mask] = converted_values
    
    gdf['event_id'] = new_event_ids
```

## 🎯 技术要点

1. **dtype=object**: 使用object类型可以同时存储整数和None值
2. **分离处理**: 分别处理有效值和空值，避免apply函数的类型推断问题  
3. **双重保险**: 在字典构建和GeoDataFrame阶段都进行类型转换

## 📊 修复效果

- ✅ `330012.0` (float) → `330012` (int)
- ✅ `NaN` (float) → `None` (object)
- ✅ PostgreSQL接受整数值，插入成功

## 🔄 渐进式查询策略（新增）

### 问题背景
部分 `data_name` 在 `transform.ods_t_data_fragment_datalake` 表中查询不到对应的 `event_id` 和 `event_name`。

### 解决方案：两阶段查询
```
阶段1：主查询 (origin_name直接查询)
data_name → transform.ods_t_data_fragment_datalake.origin_name → event_id, event_name

阶段2：备选查询 (通过defect_id间接查询) 
data_name → elasticsearch_ros.ods_ddi_index002_datalake.id → defect_id
defect_id → transform.ods_t_data_fragment_datalake.origin_source_id → event_id, event_name
```

### 查询流程
1. **主查询**：处理所有 `data_name`，记录成功和失败的数量
2. **缺失检查**：找出主查询中未找到的 `data_name`
3. **备选查询**：对缺失的 `data_name` 执行两步查询
4. **结果合并**：将主查询和备选查询结果合并

### 性能优势
- ✅ **大部分场景一次查询**：主查询成功率高时性能最优
- ✅ **按需启用备选**：只对失败的记录执行复杂查询
- ✅ **完整性保障**：最大化 `event_id` 和 `event_name` 的覆盖率
- ✅ **可监控性**：详细日志记录主/备查询使用情况

## 🔧 相关文件

- `src/spdatalab/dataset/polygon_trajectory_query.py` - 主要修复代码 + 渐进式查询策略
- `DATABASE_CONNECTION_RULES.md` - 数据库连接规范 