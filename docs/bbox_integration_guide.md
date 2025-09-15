# BBox模块集成指南

本指南介绍如何使用更新后的 `bbox.py` 模块处理 `dataset_manager` 的输出，生成边界框数据。

## 快速开始

### 标准数据集处理
```bash
# 第一阶段：构建数据集
python -m spdatalab build_dataset --input data/index.txt --dataset-name "my_dataset" --output dataset.json

# 第二阶段：处理边界框（默认启用分表模式）
python -m spdatalab process_bbox --input dataset.json
```

### 问题单数据集处理
```bash
# 第一阶段：构建问题单数据集
python -m spdatalab build_dataset --input defect_urls.txt --dataset-name "defect_dataset" --output defect_dataset.json --defect-mode

# 第二阶段：处理边界框（自动识别并创建动态字段）
python -m spdatalab process_bbox --input defect_dataset.json
```


## 处理流程概述 ⭐ **UPDATED**

系统采用**两阶段处理流程**，实现了数据集构建与边界框处理的分离：

```
原始数据文件                  统一Dataset文件               PostgreSQL数据库
    |                            |                           |
    |                            |                           |
┌───────────┐               ┌─────────────┐              ┌─────────────┐
│索引文件   │               │dataset.json │              │分表+ 动态字段│
│或         │  DatasetManager │或          │  BBox模块     │+           │
│问题单URLs │  ────────────► │dataset.     │ ────────────► │几何边界框   │
│          │               │parquet      │              │+           │
└───────────┘               └─────────────┘              │统一视图     │
                                                         └─────────────┘
第一阶段                         中间输出                    第二阶段
- URL解析                       - 统一格式                  - 自动识别类型
- 数据库查询                     - 包含metadata              - 动态创建表结构
- 属性提取                       - 场景ID列表                - 边界框查询
                                                          - 数据插入
```

### 第一阶段：DatasetManager（数据集构建）
- 处理原始数据文件（标准索引文件或问题单URL文件）
- 执行数据库查询和URL解析
- 生成统一的dataset文件（JSON/Parquet格式）

### 第二阶段：BBox模块（边界框处理）
- 读取dataset文件，自动识别数据类型
- 根据metadata动态创建表结构
- 查询边界框信息并插入数据库

## 功能特性

更新后的 `bbox.py` 模块支持以下功能：

1. **统一输入格式**：
   - 只接受dataset文件作为输入（JSON/Parquet格式）
   - 自动识别数据集类型（标准数据集 vs 问题单数据集）
   - 向后兼容文本格式（直接场景ID列表）

2. **问题单数据集支持** ⭐ **NEW**：
   - 灵活的数据库表结构，支持动态字段
   - 问题单特有字段自动识别和处理
   - 标准数据集和问题单数据集的统一处理
   - 视图中的智能数据类型分离

3. **智能进度跟踪**：
   - 轻量级Parquet状态文件
   - 自动断点续传
   - 失败记录和重试机制
   - 内存缓冲优化

4. **性能优化**：
   - 批量数据库插入
   - 错误处理和恢复机制
   - 详细的进度报告
   - 高效的文件I/O

5. **并行处理支持**：
   - 多进程并行处理 (ProcessPoolExecutor)
   - 自动CPU核心数检测
   - 手动worker数量控制
   - 按子数据集并行分片处理

6. **智能格式检测**：
   - 自动识别输入文件格式
   - 无需手动指定格式

## 使用方法

### 命令行使用

#### 基本用法
```bash
# 处理JSON格式的数据集文件（推荐方式）
python -m spdatalab process_bbox --input output/dataset.json

# 自定义批次大小
python -m spdatalab process_bbox --input output/dataset.json --batch 1000 --insert-batch 500

# 处理Parquet格式的数据集文件
python -m spdatalab process_bbox --input output/dataset.parquet --batch 2000 --insert-batch 1000

# 启用并行处理（高性能）
python -m spdatalab process_bbox --input output/dataset.json --parallel --workers 4
```

#### 问题单数据集处理 ⭐ **NEW**
```bash
# 第一步：使用dataset_manager生成问题单数据集
python -m spdatalab build_dataset \
    --input defect_urls.txt \
    --dataset-name "defect_analysis_2024" \
    --output defect_dataset.json \
    --defect-mode

# 第二步：处理问题单数据集 - 自动识别并创建动态字段
python -m spdatalab process_bbox --input defect_dataset.json --batch 500 --insert-batch 500


# 并行处理已生成的问题单数据集（高性能模式）
python -m spdatalab process_bbox \
    --input defect_dataset.json \
    --parallel \
    --workers 4 \
    --batch 500 \
    --insert-batch 500
```

#### 并行处理用法

```bash
# 基础处理（默认启用分表模式）
python -m spdatalab process_bbox --input dataset.json

# 启用并行处理（自动检测CPU核心数）
python -m spdatalab process_bbox \
    --input dataset.json \
    --parallel

# 手动指定使用4个并行worker
python -m spdatalab process_bbox \
    --input dataset.json \
    --parallel \
    --workers 4

# 高性能并行处理完整配置
python -m spdatalab process_bbox \
    --input dataset.json \
    --parallel \
    --workers 6 \
    --batch 2000 \
    --insert-batch 1000 \
    --work-dir ./parallel_logs

# 禁用分表模式（不推荐，仅用于兼容）
python -m spdatalab process_bbox \
    --input dataset.json \
    --no-partitioning
```

#### 进度跟踪和恢复
```bash
# 指定工作目录（存储进度文件）
python -m spdatalab process_bbox \
  --input output/large_dataset.json \
  --batch 1000 \
  --work-dir ./logs/bbox_import_20231201

# 程序中断后，重新运行相同命令即可自动续传
python -m spdatalab process_bbox \
  --input output/large_dataset.json \
  --batch 1000 \
  --work-dir ./logs/bbox_import_20231201

# 只重试失败的数据
python -m spdatalab process_bbox \
  --input output/large_dataset.json \
  --retry-failed \
  --work-dir ./logs/bbox_import_20231201

# 查看处理统计信息
python -m spdatalab process_bbox \
  --input output/large_dataset.json \
  --show-stats \
  --work-dir ./logs/bbox_import_20231201
```

### 参数说明

#### 基本参数
- `--input`: 输入文件路径，支持JSON/Parquet/文本格式
- `--batch`: 处理批次大小，默认1000（每批从数据库获取多少个场景的信息）
- `--insert-batch`: 插入批次大小，默认1000（每批向数据库插入多少条记录）
- `--create-table`: 是否创建表（如果不存在），默认启用

#### 进度跟踪参数
- `--work-dir`: 工作目录，默认为 `./bbox_import_logs`，用于存储进度文件
- `--retry-failed`: 只重试失败的数据，不处理已成功的数据
- `--show-stats`: 显示处理统计信息并退出，不执行实际处理

#### 并行处理参数
- `--use-parallel`: 启用并行处理模式
- `--max-workers`: 并行worker数量，默认为CPU核心数
- `--use-partitioning`: 启用分表模式（并行处理必需）
- `--create-unified-view`: 创建统一视图以便查询

### 编程方式使用

#### 标准数据集的两阶段处理

```python
from spdatalab.dataset.dataset_manager import DatasetManager
from spdatalab.dataset.bbox import run as bbox_run

# === 第一阶段：使用DatasetManager构建标准数据集 ===
# 1. 构建数据集
manager = DatasetManager()
dataset = manager.build_dataset_from_index("data/index.txt", "my_dataset")

# 2. 保存为统一的dataset文件
manager.save_dataset(dataset, "output/dataset.json", format='json')

# === 第二阶段：使用BBox模块处理dataset文件 ===
# 3. bbox模块读取dataset文件并处理（基本用法）
bbox_run(
    input_path="output/dataset.json",  # 统一的dataset文件输入
    batch=1000,
    insert_batch=500
)

# 4. 带进度跟踪的处理
bbox_run(
    input_path="output/dataset.json",
    batch=1000,
    insert_batch=500,
    work_dir="./logs/my_import",
    create_table=True
)

# 5. 重试失败的数据
bbox_run(
    input_path="output/dataset.json",
    batch=1000,
    insert_batch=500,
    retry_failed=True,
    work_dir="./logs/my_import"
)

# 6. 查看统计信息
bbox_run(
    input_path="output/dataset.json",
    show_stats=True,
    work_dir="./logs/my_import"
)
```

#### 问题单数据集的编程方式使用 ⭐ **NEW**

```python
from spdatalab.dataset.dataset_manager import DatasetManager
from spdatalab.dataset.bbox import run_with_partitioning

# === 第一阶段：使用DatasetManager构建问题单数据集 ===

# 1. 构建问题单数据集
manager = DatasetManager(defect_mode=True)
defect_dataset = manager.build_dataset_from_index(
    "data/defect_urls.txt", 
    "defect_analysis_2024",
    description="问题单数据集用于缺陷分析",
    defect_mode=True
)

# 2. 保存问题单数据集
manager.save_dataset(defect_dataset, "output/defect_dataset.json", format='json')

# 查看问题单数据集统计
stats = manager.get_dataset_stats(defect_dataset)
print(f"问题单数据集统计:")
print(f"- 总子数据集数: {stats['subdataset_count']}")
print(f"- 总场景数: {stats['total_unique_scenes']}")

# === 第二阶段：使用BBox模块处理数据集文件 ===

# 3. 处理问题单数据集（bbox模块自动识别类型并创建动态字段）
run_with_partitioning(
    input_path="output/defect_dataset.json",  # 输入dataset文件
    batch=500,
    insert_batch=500,
    use_parallel=True,
    max_workers=4,
    create_unified_view_flag=True
)

# === 处理包含额外属性的问题单数据 ===

# 处理包含额外属性的问题单文件
# 输入文件格式：url|priority=high|category=lane_detection
mixed_dataset = manager.build_dataset_from_index(
    "data/defect_urls_with_attrs.txt",
    "defect_with_attributes",
    defect_mode=True
)
manager.save_dataset(mixed_dataset, "output/mixed_defect_dataset.json", format='json')

# bbox模块处理包含动态字段的数据集
run_with_partitioning(
    input_path="output/mixed_defect_dataset.json",  # 统一的dataset文件输入
    batch=300,
    insert_batch=300,
    use_parallel=True,
    max_workers=3,
    create_unified_view_flag=True
)
```

## 进度跟踪系统

### 文件结构
```
work_dir/
├── successful_tokens.parquet  # 成功处理的场景ID记录
├── failed_tokens.parquet      # 失败记录详情
└── progress.json              # 总体进度信息（人类可读）
```

### 状态文件说明

#### successful_tokens.parquet
记录成功处理的场景信息：
- `scene_token`: 场景ID
- `processed_at`: 处理时间
- `batch_num`: 批次号

#### failed_tokens.parquet
记录失败的详细信息：
- `scene_token`: 失败的场景ID
- `error_msg`: 错误信息
- `batch_num`: 批次号
- `step`: 失败步骤（fetch_meta、fetch_bbox、data_merge、database_insert）
- `failed_at`: 失败时间

#### progress.json
总体进度概览：
```json
{
  "total_scenes": 400000,
  "processed_scenes": 350000,
  "inserted_records": 348500,
  "current_batch": 350,
  "timestamp": "2023-12-01T15:30:45",
  "successful_count": 348500,
  "failed_count": 1500
}
```

### 性能优势

| 数据规模 | 传统方案 | 新方案 | 性能提升 |
|----------|----------|--------|----------|
| 400万场景 | 400MB文本 | 80MB Parquet | 5x压缩 |
| 启动时间 | 10秒+ | 3秒 | 3x更快 |
| 查询速度 | O(n)线性 | O(1)内存查询 | 100x更快 |
| 内存占用 | 400MB+ | 50-100MB | 4x更少 |

## 问题单数据集支持详解 ⭐ **NEW**

### 核心特性

1. **合并架构**：⭐ **方案A优势** - 一个文件生成一个子数据集，大大减少表数量
2. **动态表结构**：根据问题单数据的metadata自动创建表字段
3. **灵活字段支持**：支持自定义属性字段，如priority、category等
4. **数据类型智能推断**：自动识别字段类型（text、integer、boolean等）
5. **视图分离**：问题单数据表默认不包含在统一视图中，避免数据混淆
6. **场景级属性**：个别属性存储在scene_attributes中，保持数据完整性

### 数据结构示例（方案A架构）

#### 问题单数据集的数据结构（JSON格式）
```json
{
  "name": "DefectDataset",
  "description": "问题单数据集",
  "metadata": {
    "data_type": "defect",
    "source_file": "defect_urls.txt"
  },
  "subdatasets": [
    {
      "name": "DefectDataset_defects",
      "obs_path": "defect_urls.txt",
      "duplication_factor": 1,
      "scene_count": 3,
      "scene_ids": [
        "632c1e86c95a42c9a3b6c83257ed3f82",
        "632c1e86c95a42c9a3b6c83257ed3f83",
        "632c1e86c95a42c9a3b6c83257ed3f84"
      ],
      "metadata": {
        "data_type": "defect",
        "source_file": "defect_urls.txt",
        "scene_attributes": {
          "632c1e86c95a42c9a3b6c83257ed3f82": {
            "original_url": "https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=10000_ddi-application-667754027299119535",
            "data_name": "10000_ddi-application-667754027299119535",
            "line_number": 1,
            "priority": "high",
            "category": "lane_detection"
          },
          "632c1e86c95a42c9a3b6c83257ed3f83": {
            "original_url": "https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=10000_ddi-application-667754027299119536",
            "data_name": "10000_ddi-application-667754027299119536", 
            "line_number": 2,
            "priority": "medium",
            "category": "object_detection",
            "severity": 3
          },
          "632c1e86c95a42c9a3b6c83257ed3f84": {
            "original_url": "https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=10000_ddi-application-667754027299119537",
            "data_name": "10000_ddi-application-667754027299119537",
            "line_number": 3,
            "priority": "low"
          }
        }
      }
    }
  ]
}
```

#### 架构优势对比

| 特性 | 旧架构（每URL一个子数据集） | 新架构（方案A） |
|------|---------------------------|-----------------|
| 表数量 | 100个URL → 100个表 | 100个URL → 1个表 |
| 管理复杂度 | 高 | 低 |
| 查询效率 | 需要多表联合查询 | 单表查询 |
| 属性完整性 | 分散在各个子数据集中 | 统一存储在scene_attributes中 |
| 视图管理 | 需要处理大量表 | 简化视图管理 |

### 数据表结构对比

#### 标准数据集表结构
```sql
CREATE TABLE clips_bbox_standard_dataset (
    id serial PRIMARY KEY,
    scene_token text,
    data_name text UNIQUE,
    event_id text,
    city_id text,
    timestamp bigint,
    all_good boolean,
    data_type text DEFAULT 'standard',
    geometry geometry(POLYGON, 4326)
);
```

#### 问题单数据集表结构（动态字段）
```sql
CREATE TABLE clips_bbox_defect_dataset (
    id serial PRIMARY KEY,
    scene_token text,
    data_name text UNIQUE,
    event_id text,
    city_id text,
    timestamp bigint,
    all_good boolean,
    data_type text DEFAULT 'defect',
    original_url text,
    line_number integer,
    -- 动态字段示例
    priority text,
    category text,
    severity integer,
    geometry geometry(POLYGON, 4326)
);
```

### 输入文件格式

#### 基本问题单URL文件
```
https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=10000_ddi-application-667754027299119535
https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=10000_ddi-application-667754027299119536
```

#### 包含额外属性的问题单文件
```
https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=10000_ddi-application-667754027299119535|priority=high|category=lane_detection
https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=10000_ddi-application-667754027299119536|priority=medium|category=object_detection|severity=3
```

### 数据处理流程

#### Dataset Manager阶段（数据集构建）
1. **URL解析**：从问题单URL中提取`dataName`参数
2. **数据库查询**：
   - 第一步：通过`dataName`查询`elasticsearch_ros.ods_ddi_index002_datalake`获取`defect_id`
   - 第二步：通过`defect_id`查询`transform.ods_t_data_fragment_datalake`获取`scene_id`
3. **属性解析**：解析URL中的额外属性（priority、category等）
4. **数据集生成**：**⭐ 采用合并架构**
   - 一个文件中的所有URL合并为一个子数据集
   - 所有scene_ids存储在子数据集的scene_ids列表中
   - 个别属性存储在metadata的scene_attributes中
   - 将结果保存为dataset文件（JSON/Parquet格式）

#### BBox模块阶段（边界框处理）
1. **数据集读取**：读取dataset文件，自动识别数据类型
2. **动态字段检测**：分析metadata中scene_attributes的所有字段
3. **表结构创建**：根据检测到的字段动态创建表结构
4. **边界框查询**：查询PostGIS数据库获取边界框信息
5. **数据插入**：
   - 为每个场景从scene_attributes中获取对应属性
   - 将标准字段和场景特定字段一起插入数据库

### 视图管理

#### 标准数据集统一视图
```sql
-- 只包含标准数据集的统一视图
CREATE VIEW clips_bbox_unified AS
SELECT * FROM clips_bbox_standard_dataset_1
UNION ALL
SELECT * FROM clips_bbox_standard_dataset_2
-- 问题单数据表被自动排除
```

#### 问题单数据集专用视图
```sql
-- 专门用于问题单数据的视图
CREATE VIEW clips_bbox_defect_unified AS
SELECT * FROM clips_bbox_defect_dataset_1
UNION ALL
SELECT * FROM clips_bbox_defect_dataset_2
-- 包含所有动态字段
```

### 使用建议

1. **批次大小**：问题单数据建议使用较小的批次（500-1000）
2. **表数量控制**：⭐ **方案A优势** - 一个文件生成一个表，大大减少数据库表数量
3. **并行处理**：问题单数据集适合并行处理，按子数据集分片处理
4. **错误处理**：问题单数据源可能不稳定，建议启用详细的错误跟踪
5. **字段规划**：提前规划好额外属性字段，避免后续表结构变更
6. **属性管理**：**⭐ 新架构** - 利用scene_attributes存储场景级别的自定义属性

### 查询示例

```sql
-- 查询高优先级的问题单
SELECT * FROM clips_bbox_defect_dataset 
WHERE priority = 'high' AND data_type = 'defect';

-- 按类别统计问题单
SELECT category, COUNT(*) as count 
FROM clips_bbox_defect_dataset 
WHERE data_type = 'defect' 
GROUP BY category;

-- 查询特定严重级别的问题
SELECT * FROM clips_bbox_defect_dataset 
WHERE severity >= 3 AND data_type = 'defect';
```

## 完整工作流程

### 方案1：大规模标准数据集处理（推荐）

```python
# === 第一阶段：DatasetManager构建数据集 ===
# 步骤1：构建标准数据集
manager = DatasetManager()
dataset = manager.build_dataset_from_index(
    index_file="data/training_index.txt",
    dataset_name="training_dataset_v1",
    description="训练数据集v1 - 400万场景"
)

# 步骤2：保存为Parquet格式（推荐大数据集）
manager.save_dataset(dataset, "output/training_dataset.parquet", format='parquet')

# === 第二阶段：BBox模块处理dataset文件 ===
# 步骤3：bbox模块读取dataset文件生成边界框数据（带进度跟踪）
bbox_run(
    input_path="output/training_dataset.parquet",  # 统一的dataset文件输入
    batch=1000,         # 每批处理1000个场景
    insert_batch=1000,  # 每批插入1000条记录
    work_dir="./logs/training_import",  # 进度文件目录
    create_table=True   # 自动创建表
)

# 如果中途中断，重新运行即可自动续传
# bbox_run(input_path="output/training_dataset.parquet", work_dir="./logs/training_import")
```

### 方案2：失败恢复处理

```python
# === 使用已生成的dataset文件进行失败恢复 ===
# 查看处理统计
bbox_run(
    input_path="output/dataset.parquet",  # 统一的dataset文件输入
    show_stats=True,
    work_dir="./logs/import"
)

# 输出示例：
# === 处理统计信息 ===
# 成功处理: 3,850,000 个场景
# 失败场景: 15,000 个
# 
# 按步骤分类的失败统计:
#   fetch_bbox: 8,000 个
#   database_insert: 4,200 个
#   fetch_meta: 2,800 个

# 重试失败的数据
bbox_run(
    input_path="output/dataset.parquet",  # 统一的dataset文件输入
    retry_failed=True,
    work_dir="./logs/import",
    batch=500  # 减小批次大小，提高成功率
)
```

### 方案3：JSON格式工作流程

```python
# === 第一阶段：构建中小型数据集 ===
# 适合中小型数据集或需要人类可读格式
manager = DatasetManager()
dataset = manager.build_dataset_from_index(
    index_file="data/validation_index.txt",
    dataset_name="validation_dataset_v1",
    description="验证数据集v1"
)

# 保存为JSON格式
manager.save_dataset(dataset, "output/validation_dataset.json", format='json')

# === 第二阶段：bbox模块处理dataset文件 ===
# 生成边界框数据
bbox_run(
    input_path="output/validation_dataset.json",  # 统一的dataset文件输入
    batch=500,          
    insert_batch=500,
    work_dir="./logs/validation_import"
)
```

### 方案4：问题单数据集工作流程 ⭐ **NEW**

```python
from spdatalab.dataset.dataset_manager import DatasetManager
from spdatalab.dataset.bbox import run_with_partitioning

# === 第一阶段：使用DatasetManager构建问题单数据集 ===
# 步骤1：构建问题单数据集
manager = DatasetManager(defect_mode=True)
defect_dataset = manager.build_dataset_from_index(
    index_file="data/defect_urls.txt",
    dataset_name="defect_analysis_2024_q1",
    description="2024年Q1问题单数据集，包含优先级和类别信息",
    defect_mode=True
)

# 步骤2：保存问题单数据集（推荐JSON格式便于查看）
manager.save_dataset(defect_dataset, "output/defect_dataset.json", format='json')

# === 第二阶段：使用BBox模块处理数据集文件 ===
# 步骤3：bbox模块读取dataset文件并自动识别类型，创建动态字段
run_with_partitioning(
    input_path="output/defect_dataset.json",  # 统一的dataset文件输入
    batch=500,                    # 问题单数据建议使用较小批次
    insert_batch=500,
    work_dir="./logs/defect_import",
    create_unified_view_flag=True,  # 创建统一视图
    use_parallel=True,             # 启用并行处理
    max_workers=4                  # 限制并行数量
)

# 步骤4：查看处理结果统计
print("\n=== 问题单数据集处理统计 ===")
stats = manager.get_dataset_stats(defect_dataset)
print(f"总问题单数: {stats['total_unique_scenes']}")
print(f"子数据集数: {stats['subdataset_count']}")

# 步骤5：验证数据库中的结果
import psycopg2
conn = psycopg2.connect("host=local_pg dbname=postgres user=postgres password=postgres")
cur = conn.cursor()

# 查看问题单数据表
cur.execute("""
    SELECT table_name FROM information_schema.tables 
    WHERE table_name LIKE 'clips_bbox_%' AND table_name != 'clips_bbox'
    ORDER BY table_name;
""")
tables = cur.fetchall()
print(f"\n创建的数据表: {[t[0] for t in tables]}")

# 查看问题单数据分布
cur.execute("""
    SELECT data_type, COUNT(*) as count 
    FROM clips_bbox_defect_analysis_2024_q1 
    GROUP BY data_type;
""")
data_types = cur.fetchall()
print(f"数据类型分布: {dict(data_types)}")

cur.close()
conn.close()
```

### 方案5：高级问题单数据集处理（包含自定义字段）

```python
# === 第一阶段：处理包含自定义属性的问题单文件 ===
# 输入文件格式：url|priority=high|category=lane_detection|severity=3

manager = DatasetManager(defect_mode=True)
advanced_defect_dataset = manager.build_dataset_from_index(
    index_file="data/defect_urls_with_attributes.txt",
    dataset_name="advanced_defect_2024",
    description="包含优先级、类别和严重程度的问题单数据集",
    defect_mode=True
)

# 保存为统一的dataset文件格式
manager.save_dataset(advanced_defect_dataset, "output/advanced_defect_dataset.json", format='json')

# === 第二阶段：bbox模块处理dataset文件 ===
# bbox模块自动识别metadata中的动态字段并创建相应表结构
run_with_partitioning(
    input_path="output/advanced_defect_dataset.json",  # 统一的dataset文件输入
    batch=300,                     # 更小的批次，因为包含更多字段
    insert_batch=300,
    work_dir="./logs/advanced_defect_import",
    create_unified_view_flag=True,
    use_parallel=True,
    max_workers=3
)

# 验证动态字段创建
conn = psycopg2.connect("host=local_pg dbname=postgres user=postgres password=postgres")
cur = conn.cursor()

# 查看表结构
cur.execute("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name = 'clips_bbox_advanced_defect_2024'
    ORDER BY ordinal_position;
""")
columns = cur.fetchall()
print(f"\n动态创建的表结构:")
for col_name, col_type in columns:
    print(f"  {col_name}: {col_type}")

# 查看数据分布
cur.execute("""
    SELECT priority, category, COUNT(*) as count 
    FROM clips_bbox_advanced_defect_2024 
    WHERE data_type = 'defect'
    GROUP BY priority, category
    ORDER BY count DESC;
""")
distribution = cur.fetchall()
print(f"\n问题单分布:")
for priority, category, count in distribution:
    print(f"  {priority} - {category}: {count}")

cur.close()
conn.close()
```

## 性能优化建议

### 1. 批次大小调优

根据您的系统资源调整批次大小：

- **内存充足（16GB+）**：`batch=2000`, `insert_batch=2000`
- **标准配置（8GB）**：`batch=1000`, `insert_batch=1000`
- **资源受限（4GB）**：`batch=500`, `insert_batch=500`

### 2. 工作目录管理

```bash
# 为不同的导入任务创建独立目录
mkdir -p logs/imports/training_20231201
mkdir -p logs/imports/validation_20231201
mkdir -p logs/imports/test_20231201

# 使用时间戳避免冲突
python -m spdatalab process_bbox --input dataset.json --work-dir "./logs/imports/$(date +%Y%m%d_%H%M%S)"
```

### 3. 并行处理（推荐）

使用内置的并行处理功能：

```bash
# 基础并行处理
python -m spdatalab process-bbox \
    --input dataset.json \
    --use-partitioning \
    --use-parallel

# 指定并行worker数量
python -m spdatalab process-bbox \
    --input dataset.json \
    --use-partitioning \
    --use-parallel \
    --max-workers 4

# 完整的并行处理配置
python -m spdatalab process-bbox \
    --input dataset.json \
    --use-partitioning \
    --use-parallel \
    --max-workers 6 \
    --batch 2000 \
    --insert-batch 1000 \
    --create-unified-view \
    --work-dir ./parallel_logs
```

#### 并行处理的最佳实践

根据数据规模选择合适的设置：

| 数据规模 | 推荐设置 | 命令参数 |
|----------|----------|----------|
| 小数据集（<50万记录） | 顺序处理 | `--use-partitioning`（不使用`--use-parallel`） |
| 中等数据集（50万-200万记录） | 并行处理，worker数量 = CPU核心数的一半 | `--use-partitioning --use-parallel --max-workers 4` |
| 大数据集（>200万记录） | 并行处理，worker数量 = CPU核心数 | `--use-partitioning --use-parallel`（让系统自动检测） |
| 服务器环境 | 限制worker数量避免影响其他服务 | `--use-partitioning --use-parallel --max-workers 6` |
| 开发测试环境 | 使用较少worker避免系统过载 | `--use-partitioning --use-parallel --max-workers 2` |

### 4. 手动分片处理（高级用法）

对于超大型数据集（1000万+场景），可以考虑手动分片并行处理：

```python
# 分割数据集
def split_dataset(dataset, num_parts=4):
    subdatasets_per_part = len(dataset.subdatasets) // num_parts
    parts = []
    
    for i in range(num_parts):
        start_idx = i * subdatasets_per_part
        end_idx = start_idx + subdatasets_per_part if i < num_parts - 1 else len(dataset.subdatasets)
        
        part_dataset = Dataset(
            name=f"{dataset.name}_part_{i+1}",
            description=f"{dataset.description} - 第{i+1}部分",
            subdatasets=dataset.subdatasets[start_idx:end_idx]
        )
        parts.append(part_dataset)
    
    return parts

# 保存分片数据集
manager = DatasetManager()
dataset = manager.load_dataset("output/huge_dataset.parquet")
parts = split_dataset(dataset, num_parts=8)

for i, part in enumerate(parts):
    part_file = f"output/dataset_part_{i+1}.parquet"
    manager.save_dataset(part, part_file, format='parquet')
    
    # 每个分片使用独立的工作目录
    bbox_run(
        input_path=part_file,
        work_dir=f"./logs/part_{i+1}",
        batch=1000,
        insert_batch=1000
    )
```

### 5. 数据库优化

- **索引优化**：程序会自动创建必要的索引
- **唯一约束**：自动处理重复数据，支持断点续传
- **批量插入**：使用 `method='multi'` 参数进行高效批量插入

## 错误处理和恢复

### 自动恢复机制

1. **断点续传**：程序重启后自动跳过已处理的数据
2. **失败重试**：可以专门重试失败的数据
3. **批量降级**：批量插入失败时自动降级为逐行插入
4. **数据去重**：数据库层面防止重复插入

### 错误分类

程序会详细记录失败原因：

- **fetch_meta**：无法获取场景元数据
- **fetch_bbox**：无法获取边界框数据
- **data_merge**：元数据与边界框数据无法匹配
- **database_insert**：数据库插入失败

### 故障排除步骤

1. **查看统计信息**：
   ```bash
   python -m spdatalab process_bbox --input dataset.json --show-stats --work-dir ./logs
   ```

2. **检查失败记录**：
   ```python
   import pandas as pd
   failed_df = pd.read_parquet("./logs/failed_tokens.parquet")
   print(failed_df['step'].value_counts())
   ```

3. **重试失败数据**：
   ```bash
   python -m spdatalab process_bbox --input dataset.json --retry-failed --work-dir ./logs --batch 500
   ```

## 监控和日志

处理过程中会输出详细的进度信息：

```
开始处理输入文件: output/dataset.json
工作目录: ./logs/bbox_import
进度数据库初始化完成: 成功 0 个, 失败 0 个
从JSON文件加载了 4000000 个scene_id
总计 4000000 个场景，已成功处理 0 个，剩余 4000000 个
开始处理 4000000 个场景，批次大小: 1000

[批次 1] 处理 1000 个场景
[批次 1] 获取到 950 条元数据
[批次 1] 获取到 950 条边界框数据
[批次 1] 合并后得到 950 条记录
[批量插入] 已插入: 500/950 行
[批量插入] 已插入: 950/950 行
已保存 950 个成功记录到文件
[批次 1] 完成，插入 950 条记录
[累计进度] 已处理: 950, 已插入: 950

...

=== 最终统计 ===
成功处理: 3,985,000 个场景
失败场景: 15,000 个

状态文件位置:
- 成功记录: ./logs/bbox_import/successful_tokens.parquet
- 失败记录: ./logs/bbox_import/failed_tokens.parquet
- 进度文件: ./logs/bbox_import/progress.json
```

## 并行处理架构说明

### 并行化策略
- **按子数据集(subdataset)进行并行处理**：每个worker处理一个完整的子数据集
- **避免数据库连接竞争**：每个worker使用独立的数据库连接
- **事务隔离**：避免不同worker间的事务冲突

### 性能优势
- **CPU密集型任务并行化**：数据处理、转换等操作并行执行
- **I/O操作并行化**：数据库查询、插入操作并行执行
- **减少总体处理时间**：大数据集处理速度提升2-6倍

### 注意事项
- **数据库连接池限制**：每个worker需要独立的数据库连接
- **内存使用量增加**：多进程同时运行会增加内存占用
- **子数据集数量限制**：不适合子数据集数量很少的场景

### 监控指标
- **独立进度跟踪**：每个worker的处理进度独立显示
- **性能提升计算**：总体性能提升倍数自动计算
- **吞吐量对比**：处理时间和吞吐量对比统计

## 故障排除指南

### 并行处理比顺序处理慢
**可能原因**：
- 数据量太小，多进程开销大于收益
- worker数量设置过多，资源竞争激烈
- 数据库连接限制

**解决方案**：
- 减少max-workers数量
- 对小数据集使用顺序处理
- 检查数据库连接池配置

### 内存使用过高
**可能原因**：
- worker数量过多
- batch-size设置过大
- 大量数据同时加载到内存

**解决方案**：
- 减少max-workers
- 减少batch-size
- 增加系统内存或使用更小的批次处理

### 某些worker进程卡住
**可能原因**：
- 数据库连接超时
- 某个子数据集数据异常
- 进程间死锁

**解决方案**：
- 检查数据库连接状态
- 查看worker进程日志
- 重启处理，跳过问题数据集

## 性能对比测试

使用专用的性能测试脚本：

```bash
# 对比并行和顺序处理性能
python test_parallel_performance.py \
    --dataset-file dataset.json \
    --test-mode both \
    --batch-size 1000 \
    --max-workers 4

# 仅测试并行处理
python test_parallel_performance.py \
    --dataset-file dataset.json \
    --test-mode parallel \
    --max-workers 6

# 仅测试顺序处理
python test_parallel_performance.py \
    --dataset-file dataset.json \
    --test-mode sequential
```

## 常见问题

### 1. 依赖问题

**问题**：`ImportError: 需要安装 pandas 和 pyarrow`
```bash
# 解决方法
pip install pandas pyarrow
```

**问题**：`警告: 未安装pyarrow，将使用降级的文本文件模式`
- 功能仍可正常使用，但性能会降低
- 建议安装pyarrow获得最佳性能

### 2. 数据库问题

**问题**：数据库连接失败
- 检查数据库连接字符串
- 确认数据库服务正在运行
- 验证用户权限

**问题**：表已存在冲突
- 程序会自动处理表创建和索引
- 支持重复数据的优雅处理

### 3. 性能问题

**问题**：内存不足
- 减小 `batch` 和 `insert_batch` 参数
- 使用数据集分片处理

**问题**：插入速度慢
- 增大 `insert_batch` 参数
- 检查数据库性能和网络延迟

### 4. 文件问题

**问题**：工作目录权限不足
```bash
# 确保目录存在且有写权限
mkdir -p ./logs/bbox_import
chmod 755 ./logs/bbox_import
```

**问题**：磁盘空间不足
- 状态文件大小约为原数据的20%
- 400万场景约需要80-120MB磁盘空间

### 5. 问题单数据集相关问题 ⭐ **NEW**

**问题**：问题单URL解析失败
```
无法从URL中提取数据名称: https://pre-prod.adscloud.huawei.com/...
```
**解决方案**：
- 检查URL格式是否正确
- 确认URL中包含`dataName`参数
- 验证URL编码是否正确

**问题**：问题单数据库查询失败
```
查询问题单数据失败: data_name, 错误: connection timeout
```
**解决方案**：
- 检查数据库连接配置
- 确认有权限访问`elasticsearch_ros.ods_ddi_index002_datalake`和`transform.ods_t_data_fragment_datalake`
- 验证数据库服务可用性

**问题**：动态字段创建失败
```
创建分表时出错: column "priority" cannot be added
```
**解决方案**：
- 检查字段名称是否为PostgreSQL保留字
- 使用双引号包围特殊字段名
- 避免使用空格或特殊字符作为字段名

**问题**：问题单数据混入标准数据集视图
```
统一视图中包含了问题单数据
```
**解决方案**：
- 确保使用`exclude_defect_tables=True`参数
- 检查表的`data_type`字段是否正确设置
- 重新创建统一视图

**问题**：额外属性字段类型错误
```
invalid input syntax for type integer: "high"
```
**解决方案**：
- 检查输入文件中的属性值格式
- 确认数值字段不包含非数值字符
- 使用正确的分隔符格式：`url|key=value|key2=value2`

**问题**：方案A架构中scene_attributes找不到场景属性
```
KeyError: 'scene_attributes' not found in metadata
```
**解决方案**：
- 确认使用的是最新版本的DatasetManager
- 检查数据集文件的metadata结构
- 验证问题单数据集的构建过程是否正确
- 确保`defect_mode=True`参数正确使用

**问题**：合并后的子数据集表数量仍然很多
```
创建了多个问题单数据表，而不是一个
```
**解决方案**：
- 确认使用的是方案A的实现版本
- 检查是否有多个问题单文件同时处理
- 验证每个文件是否正确合并为一个子数据集
- 查看日志中的子数据集创建信息

## 示例脚本

完整的示例脚本位于 `examples/bbox_usage_example.py`，包含：

- 标准数据集两阶段处理流程
- 问题单数据集两阶段处理流程
- JSON格式完整工作流程
- Parquet格式完整工作流程
- 失败恢复处理示例
- 性能优化配置

运行示例：

```bash
# 标准数据集JSON工作流程
python examples/bbox_usage_example.py --mode json

# 标准数据集Parquet工作流程（推荐）
python examples/bbox_usage_example.py --mode parquet

# 问题单数据集工作流程
python examples/bbox_usage_example.py --mode defect

# 失败恢复示例
python examples/bbox_usage_example.py --mode recovery

# 性能测试
python examples/bbox_usage_example.py --mode benchmark
```

## 并行处理演示

运行并行处理功能演示脚本：

```bash
# 查看并行处理功能演示
python demo_parallel_commands.py
```

该脚本包含：
- 并行处理命令演示
- 性能对比测试方法
- 最佳实践建议
- 架构说明
- 故障排除指南

## 关键新功能总结

### 架构改进
✅ **两阶段处理流程** ⭐ **NEW**  
✅ **职责分离设计** (DatasetManager + BBox模块)  
✅ **统一dataset文件输入**  
✅ **自动数据类型识别**  

### 性能功能
✅ **多进程并行处理** (ProcessPoolExecutor)  
✅ **自动CPU核心数检测**  
✅ **手动worker数量控制**  
✅ **独立进度跟踪**  
✅ **性能提升监控**  
✅ **智能错误处理**  

### 问题单数据集支持
✅ **问题单数据集支持** ⭐ **NEW**  
✅ **动态表结构创建**  
✅ **灵活字段类型推断**  
✅ **数据类型智能分离**  

### 预期性能提升

- **大数据集处理速度提升 2-6倍**
- **CPU资源充分利用**
- **总体处理时间显著减少**

### 两阶段处理流程优势

- **职责清晰分离**：数据集构建与边界框处理分离
- **输入格式统一**：bbox模块只处理dataset文件，简化使用
- **灵活扩展**：支持不同数据源（标准数据集、问题单等）
- **向后兼容**：原有处理方式完全保留

### 问题单数据集新功能

- **合并架构（方案A）** ⭐ **NEW**：一个文件生成一个子数据集，大大减少表数量
- **自动URL解析和数据库查询**（DatasetManager阶段）
- **动态字段创建和类型推断**（BBox模块阶段）
- **标准数据集与问题单数据集分离**
- **场景级属性存储**：个别属性存储在scene_attributes中，保持完整性
- **灵活的属性支持（priority、category等）**
- **专用视图管理，避免数据混淆**

### 相关文件

#### 核心模块
- **BBox模块**: `src/spdatalab/dataset/bbox.py` - 边界框处理，支持动态字段
- **DatasetManager**: `src/spdatalab/dataset/dataset_manager.py` - 数据集构建，支持问题单处理

#### 文档
- **数据集管理文档**: `docs/dataset_management.md` - DatasetManager详细使用指南
- **开发计划**: `DEVELOPMENT_PLAN.md` - 整体开发计划和进度

#### 测试和演示
- **性能测试**: `python test_parallel_performance.py` - 并行处理性能测试
- **并行处理演示**: `demo_parallel_commands.py` - 并行处理功能演示
- **使用示例**: `examples/bbox_usage_example.py` - 完整使用示例

#### CLI命令
- **构建数据集**: `python -m spdatalab build_dataset` - DatasetManager命令行接口
- **处理边界框**: `python -m spdatalab process-bbox` - BBox模块命令行接口 