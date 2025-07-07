# 问题单URL处理功能总结

## 功能概述

本次更新为数据集管理器添加了专门的问题单URL处理功能，支持从包含额外属性的URL文件中生成bbox数据，包括：

1. **URL跳转支持** - 在bbox表中增加URL列，方便用户跳转到原始问题单页面
2. **责任模块信息** - 支持责任模块字段，便于按团队/模块分析问题
3. **问题描述信息** - 支持问题描述字段，提供更详细的上下文信息
4. **独立表结构** - 使用专门的表结构，不与其他数据混合

## 主要改动

### 1. `dataset_manager.py` 修改

**新增方法：**
- `parse_url_line_with_attributes()` - 解析包含额外属性的URL行
- `extract_scene_ids_from_urls_with_attributes()` - 从URL文件提取场景数据及属性
- 修改 `detect_file_format()` - 增加 `url_with_attributes` 格式检测
- 修改 `extract_scene_ids_from_file()` - 支持带属性URL格式的自动处理

**功能特点：**
- 支持Tab分隔的URL文件格式：`URL\t责任模块\t问题描述`
- 自动解析URL中的dataName参数
- 保持向后兼容，支持纯URL格式
- 智能格式检测，自动选择合适的处理方式

### 2. `bbox.py` 修改

**新增功能：**
- `create_issue_bbox_table_if_not_exists()` - 创建问题单专用表结构
- `batch_insert_issue_data_to_postgis()` - 批量插入问题单数据
- `extract_scene_data_with_attributes_from_url_file()` - 提取场景数据和属性
- `run_issue_tickets_processing()` - 问题单专用处理流程
- 新增命令行参数：`--issue-tickets` 和 `--issue-table`

**表结构：**
```sql
CREATE TABLE clips_bbox_issues(
    id serial PRIMARY KEY,
    scene_token text,
    data_name text UNIQUE,
    event_id text,
    city_id text,
    "timestamp" bigint,
    all_good boolean,
    url text NOT NULL,              -- 原始URL
    module text DEFAULT '',         -- 责任模块
    description text DEFAULT '',    -- 问题描述
    dataname text,                  -- dataName参数
    defect_id text,                 -- defect_id
    geometry geometry(POLYGON, 4326) -- 几何信息
);
```

### 3. 新增文档和示例

**文档文件：**
- `docs/issue_tickets_bbox_guide.md` - 详细使用指南
- `docs/issue_tickets_feature_summary.md` - 功能总结（本文件）

**示例文件：**
- `examples/issue_tickets_example.txt` - URL文件格式示例
- `examples/issue_tickets_processing_example.py` - Python使用示例

## 使用方法

### 1. 准备URL文件

创建包含问题单URL的文本文件，支持两种格式：

**基本格式（只有URL）：**
```
https://example.com/detail?dataName=10000_test
https://example.com/detail?dataName=20000_test
```

**增强格式（含额外属性）：**
```
https://example.com/detail?dataName=10000_test	基础巡航	"左转窄桥问题"
https://example.com/detail?dataName=20000_test	智能泊车	"泊车精度不足"
```

### 2. 运行处理命令

```bash
# 使用问题单专用模式
python -m spdatalab.dataset.bbox \
    --input data/issue_tickets.txt \
    --issue-tickets \
    --create-table \
    --batch 100

# 指定自定义表名
python -m spdatalab.dataset.bbox \
    --input data/problem_tickets_20250703.txt \
    --issue-tickets \
    --issue-table clips_bbox_issues_20250703 \
    --create-table
```

### 3. 查询和分析

```sql
-- 查看所有问题单数据
SELECT scene_token, url, module, description, all_good
FROM clips_bbox_issues
ORDER BY id;

-- 按责任模块统计
SELECT module, COUNT(*) as count
FROM clips_bbox_issues
WHERE module != ''
GROUP BY module;

-- 空间查询
SELECT url, module, description
FROM clips_bbox_issues
WHERE ST_Intersects(geometry, ST_MakeEnvelope(116.3, 39.9, 116.4, 40.0, 4326));
```

## 技术实现

### 数据流程

1. **URL解析** - 从URL中提取dataName参数
2. **数据库查询** - 通过dataName查询defect_id，再查询scene_ids
3. **属性映射** - 将URL、模块、描述等属性映射到scene_id
4. **数据合并** - 合并元数据、边界框数据和属性信息
5. **数据插入** - 插入到专门的问题单表中

### 关键特性

- **自动格式检测** - 智能识别URL文件格式（基本/增强）
- **容错处理** - 缺失属性时使用默认值，保证数据完整性
- **进度跟踪** - 详细的处理日志和进度文件
- **重复处理** - 自动跳过已处理的数据，支持断点续传
- **独立存储** - 不影响现有的parquet等其他格式数据处理

## 兼容性说明

### 向后兼容
- 原有的数据处理功能完全不受影响
- 原有的parquet、JSON、传统URL格式继续正常工作
- 分表模式和统一视图创建不包含问题单表，保持独立

### 数据库要求
- 需要PostgreSQL + PostGIS环境
- 需要访问elasticsearch_ros和transform数据库的权限
- 使用与现有bbox功能相同的数据库连接配置

## 使用场景

1. **问题追踪** - 通过URL直接跳转到问题单详情页面
2. **团队分析** - 按责任模块统计和分析问题分布
3. **空间分析** - 结合地理位置分析问题热点区域
4. **质量评估** - 通过all_good字段评估问题解决情况
5. **历史记录** - 保留完整的问题描述和处理记录

## 未来扩展

- 支持更多自定义字段
- 集成QGIS可视化支持
- 添加问题单状态追踪
- 支持批量URL导出功能
- 添加问题单分类和标签系统 