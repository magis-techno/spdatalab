# Sprint 2 分表模式使用指南

## 概述

Sprint 2 引入了分表模式，允许按子数据集分表存储边界框数据，解决了大规模数据的存储和查询性能问题。

## 主要功能

### 1. 分表模式处理
- 按子数据集自动创建分表
- 独立的进度跟踪（每个子数据集一个工作目录）
- 统一视图自动聚合所有分表数据

### 2. 统一视图管理
- 自动发现所有bbox分表
- 动态创建统一视图
- 支持跨表查询

### 3. 表管理工具
- 列出所有bbox表
- 视图维护和更新

## 使用方法

### 1. 分表模式处理数据

#### 基本用法
```bash
# 使用分表模式处理数据集
python -m spdatalab process-bbox \
    --input dataset.json \
    --use-partitioning \
    --batch 1000 \
    --insert-batch 1000
```

#### 完整参数
```bash
# 带所有参数的分表模式
python -m spdatalab process-bbox \
    --input dataset.json \
    --use-partitioning \
    --create-unified-view \
    --batch 1000 \
    --insert-batch 1000 \
    --work-dir ./my_bbox_logs
```

#### 仅维护统一视图（不处理数据）
```bash
# 只更新统一视图，不处理数据
python -m spdatalab process-bbox \
    --input dataset.json \
    --use-partitioning \
    --maintain-view-only
```

### 2. 独立的视图管理命令

#### 创建统一视图
```bash
# 创建默认的统一视图
python -m spdatalab create-unified-view

# 创建自定义名称的统一视图
python -m spdatalab create-unified-view --view-name my_custom_view
```

#### 维护统一视图
```bash
# 维护默认统一视图
python -m spdatalab maintain-unified-view

# 维护自定义视图
python -m spdatalab maintain-unified-view --view-name my_custom_view
```

#### 列出所有bbox表
```bash
# 显示所有bbox相关表
python -m spdatalab list-bbox-tables
```

### 3. 传统模式（向下兼容）

```bash
# 传统单表模式（默认）
python -m spdatalab process-bbox \
    --input dataset.json \
    --batch 1000 \
    --insert-batch 1000
```

## 分表模式 vs 传统模式

| 特性 | 传统模式 | 分表模式 |
|------|----------|----------|
| 数据存储 | 单表 `clips_bbox` | 按子数据集分表 |
| 查询性能 | 大数据量时性能下降 | 良好的分表查询性能 |
| 并行处理 | 单线程顺序处理 | 每个子数据集独立处理 |
| 进度跟踪 | 全局进度文件 | 每个子数据集独立跟踪 |
| 跨数据集查询 | 直接查询主表 | 通过统一视图查询 |
| 适用场景 | 小到中等规模数据 | 大规模数据（500万+记录） |

## 数据库表结构

### 分表命名规则

分表名称规则：`clips_bbox_{规范化的子数据集名称}`

例如：
- `clips_bbox_lane_change_2024_05_18_10_56_18`
- `clips_bbox_overtaking_main_scenario`
- `clips_bbox_traffic_light_detection`

### 统一视图结构

统一视图 `clips_bbox_unified` 包含以下字段：
- 所有原始表字段：`id`, `scene_token`, `data_name`, `event_id`, `city_id`, `timestamp`, `all_good`, `geometry`
- 额外字段：
  - `subdataset_name`: 子数据集名称
  - `source_table`: 源表名称

## QGIS集成

### 连接统一视图

1. **连接数据库**
   ```
   主机: local_pg
   端口: 5432
   数据库: postgres
   用户名: postgres
   密码: postgres
   ```

2. **加载统一视图**
   - 选择视图 `clips_bbox_unified`
   - 几何字段: `geometry`
   - 主键: `id, source_table` (复合主键)

3. **按子数据集过滤**
   ```sql
   -- 查询特定子数据集
   SELECT * FROM clips_bbox_unified 
   WHERE subdataset_name = 'lane_change_2024_05_18_10_56_18'
   
   -- 查询多个子数据集
   SELECT * FROM clips_bbox_unified 
   WHERE subdataset_name IN ('lane_change_main', 'overtaking_scenario')
   ```

## 监控和维护

### 检查表状态

```sql
-- 查看所有bbox表及记录数
SELECT 
    table_name,
    (xpath('/row/c/text()', query_to_xml(format('select count(*) as c from %I', table_name), false, true, '')))[1]::text::int as row_count
FROM information_schema.tables 
WHERE table_schema = 'public' 
AND table_name LIKE 'clips_bbox%'
ORDER BY table_name;
```

### 检查统一视图状态

```sql
-- 统一视图总览
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT subdataset_name) as subdataset_count,
    COUNT(DISTINCT source_table) as table_count
FROM clips_bbox_unified;

-- 按子数据集统计
SELECT 
    subdataset_name,
    source_table,
    COUNT(*) as record_count
FROM clips_bbox_unified
GROUP BY subdataset_name, source_table
ORDER BY record_count DESC;
```

## 故障排除

### 常见问题

1. **统一视图查询失败**
   ```bash
   # 重新创建统一视图
   python -m spdatalab maintain-unified-view
   ```

2. **分表创建失败**
   - 检查子数据集名称是否符合PostgreSQL规范
   - 检查表名长度是否超限（50字符）

3. **进度丢失**
   - 每个子数据集有独立的进度跟踪
   - 检查 `work-dir/{subdataset_name}/` 目录下的进度文件

### 调试信息

```bash
# 启用详细日志
export PYTHONPATH=src
python -c "
import logging
logging.basicConfig(level=logging.DEBUG)
from spdatalab.dataset.bbox import list_bbox_tables
from sqlalchemy import create_engine
eng = create_engine('postgresql+psycopg://postgres:postgres@local_pg:5432/postgres')
tables = list_bbox_tables(eng)
print('Found tables:', tables)
"
```

## 最佳实践

1. **数据集规模选择**
   - 小于100万记录：推荐传统模式
   - 大于100万记录：推荐分表模式

2. **批次大小设置**
   - 分表模式可以使用较大的批次 (1000-5000)
   - 每个子数据集独立处理，不会相互影响

3. **进度监控**
   - 定期检查各子数据集的处理进度
   - 使用 `--maintain-view-only` 定期更新统一视图

4. **性能优化**
   - 为高频查询的字段创建额外索引
   - 根据查询模式调整统一视图的定义

## 升级指南

### 从传统模式迁移到分表模式

1. **备份现有数据**
   ```sql
   CREATE TABLE clips_bbox_backup AS SELECT * FROM clips_bbox;
   ```

2. **运行分表模式处理**
   ```bash
   python -m spdatalab process-bbox \
       --input dataset.json \
       --use-partitioning
   ```

3. **验证数据一致性**
   ```sql
   -- 比较记录总数
   SELECT COUNT(*) FROM clips_bbox_backup;
   SELECT COUNT(*) FROM clips_bbox_unified;
   ```

4. **更新应用查询**
   - 将查询从 `clips_bbox` 改为 `clips_bbox_unified`
   - 利用新增的 `subdataset_name` 字段进行过滤

## 支持和反馈

如有问题或建议，请：
1. 检查本文档的故障排除部分
2. 运行测试脚本验证功能
3. 查看详细的日志输出

---

*更新时间: 2024-12-19*
*版本: Sprint 2* 