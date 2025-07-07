# 问题单URL处理指南

## 功能概述

新的问题单URL处理功能允许您从包含额外属性的URL文件中生成bbox数据，支持：

- URL跳转功能
- 责任模块信息
- 问题描述信息  
- 专门的数据库表结构

## 文件格式

### 支持的URL文件格式

**基本格式（只有URL）：**
```
https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=10000_ddi-application-667754027299119535
https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=90000_ddi-application-667754027299119536
```

**增强格式（含额外属性，使用Tab分隔）：**
```
https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=10000_ddi-application-667754027299119535	基础巡航	"左转窄桥，内切验证有剐蹭护栏风险"
https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=90000_ddi-application-667754027299119536	智能泊车	"自动泊车精度不足，需要优化算法"
```

**字段说明：**
- 第1列：问题单URL（必须）
- 第2列：责任模块（可选）
- 第3列：问题描述（可选，支持带引号）

## 数据库表结构

问题单数据使用专门的表结构 `clips_bbox_issues`，包含以下字段：

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

## 使用方法

### 基本用法

```bash
# 处理问题单URL文件
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

### 高级参数

```bash
python -m spdatalab.dataset.bbox \
    --input data/issue_tickets.txt \
    --issue-tickets \
    --issue-table clips_bbox_issues \
    --batch 100 \
    --insert-batch 500 \
    --work-dir ./issue_logs \
    --create-table
```

**参数说明：**
- `--issue-tickets`: 启用问题单专用处理模式
- `--issue-table`: 指定问题单表名（默认：clips_bbox_issues）
- `--create-table`: 自动创建问题单表
- `--batch`: 处理批次大小
- `--insert-batch`: 插入批次大小
- `--work-dir`: 工作目录，存储日志和进度文件

## 数据查询

### 基本查询

```sql
-- 查看所有问题单数据
SELECT 
    scene_token,
    url,
    module,
    description,
    dataname,
    all_good,
    ST_AsText(geometry) as geometry_wkt
FROM clips_bbox_issues
ORDER BY id;

-- 按责任模块分组统计
SELECT 
    module,
    COUNT(*) as count,
    COUNT(CASE WHEN all_good THEN 1 END) as good_count
FROM clips_bbox_issues
WHERE module != ''
GROUP BY module
ORDER BY count DESC;

-- 查询特定模块的问题
SELECT 
    url,
    description,
    scene_token,
    all_good
FROM clips_bbox_issues
WHERE module = '基础巡航'
ORDER BY id;
```

### 空间查询

```sql
-- 查询指定区域内的问题单
SELECT 
    url,
    module,
    description,
    scene_token
FROM clips_bbox_issues
WHERE ST_Intersects(
    geometry,
    ST_MakeEnvelope(116.3, 39.9, 116.4, 40.0, 4326)
);

-- 计算问题单覆盖面积
SELECT 
    url,
    module,
    ST_Area(ST_Transform(geometry, 3857)) as area_sqm
FROM clips_bbox_issues
WHERE geometry IS NOT NULL
ORDER BY area_sqm DESC;
```

## 特性说明

### 1. 独立表结构
- 问题单数据使用专门的表结构，不与其他数据混合
- 包含URL、责任模块、问题描述等问题单特有字段
- 不会影响现有的parquet等其他格式的数据处理

### 2. URL跳转支持
- 原始URL保存在 `url` 字段中
- 可直接点击跳转到问题单详情页面
- 便于问题追踪和分析

### 3. 属性扩展
- 支持责任模块信息，便于按团队/模块分析
- 支持问题描述，提供更详细的上下文信息
- 保留dataName和defect_id，便于数据追溯

### 4. 容错处理
- 自动检测文件格式（基本URL格式 vs 增强属性格式）
- 缺失属性时使用默认空值
- 详细的错误日志和进度跟踪

## 输出示例

处理完成后，您将看到类似以下的输出：

```
=== 问题单URL处理开始 ===
输入文件: data/issue_tickets.txt
目标表: clips_bbox_issues
工作目录: ./bbox_import_logs
批次大小: 100
插入批次大小: 1000

=== 步骤1: 提取场景数据和属性 ===
提取到 156 条场景记录

=== 步骤2: 分批处理场景数据 ===
[批次 1] 处理 100 个场景
[批次 1] 获取到 98 条元数据
[批次 1] 获取到 98 条边界框数据
[批次 1] 合并后得到 98 条记录
[问题单批量插入] 已插入: 98/98 行到 clips_bbox_issues
[批次 1] 完成，插入 98 条记录到 clips_bbox_issues

问题单处理完成！总计处理: 156 条记录，成功插入: 156 条记录

=== 最终统计 ===
成功处理: 156 个场景
失败场景: 0 个
问题单表: clips_bbox_issues
```

## 注意事项

1. **文件格式**：确保URL文件使用Tab分隔符分隔字段
2. **权限要求**：需要访问elasticsearch_ros和transform数据库的权限  
3. **表名规范**：建议使用有意义的表名，如包含日期或项目名称
4. **数据备份**：重要数据处理前建议先备份数据库
5. **网络连接**：处理过程中需要稳定的数据库连接

## 故障排除

### 常见问题

**Q: 提示"无法获取元数据"**
A: 检查数据库连接和权限，确保能访问transform.ods_t_data_fragment_datalake表

**Q: 提示"URL解析失败"**
A: 检查URL格式，确保包含dataName参数

**Q: 插入数据时出现重复键错误**
A: 系统会自动处理重复数据，跳过已存在的记录

**Q: 处理过程中断了怎么办？**
A: 可以重新运行相同命令，系统会自动跳过已处理的数据

### 日志文件

- 成功记录：`./bbox_import_logs/successful_tokens.parquet`
- 失败记录：`./bbox_import_logs/failed_tokens.parquet`  
- 进度文件：`./bbox_import_logs/progress.json` 