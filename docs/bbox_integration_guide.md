# BBox模块集成指南

本指南介绍如何使用更新后的 `bbox.py` 模块处理 `dataset_manager` 的输出，生成边界框数据。

## 概述

更新后的 `bbox.py` 模块支持以下功能：

1. **多格式输入支持**：
   - JSON格式（dataset_manager的JSON输出）
   - Parquet格式（dataset_manager的Parquet输出）
   - 文本格式（向后兼容原有格式）

2. **智能进度跟踪**：
   - 轻量级Parquet状态文件
   - 自动断点续传
   - 失败记录和重试机制
   - 内存缓冲优化

3. **性能优化**：
   - 批量数据库插入
   - 错误处理和恢复机制
   - 详细的进度报告
   - 高效的文件I/O

4. **并行处理支持**：
   - 多进程并行处理 (ProcessPoolExecutor)
   - 自动CPU核心数检测
   - 手动worker数量控制
   - 按子数据集并行分片处理

5. **智能格式检测**：
   - 自动识别输入文件格式
   - 无需手动指定格式

## 使用方法

### 命令行使用

#### 基本用法
```bash
# 处理JSON格式的数据集文件
python src/spdatalab/dataset/bbox.py --input output/dataset.json --batch 1000 --insert-batch 500

# 处理Parquet格式的数据集文件
python src/spdatalab/dataset/bbox.py --input output/dataset.parquet --batch 2000 --insert-batch 1000

# 处理文本格式的场景ID列表（向后兼容）
python src/spdatalab/dataset/bbox.py --input data/scene_ids.txt --batch 500
```

#### 并行处理用法

```bash
# 使用默认设置开启并行处理（自动检测CPU核心数）
python -m spdatalab process-bbox \
    --input dataset.json \
    --use-partitioning \
    --use-parallel

# 手动指定使用4个并行worker
python -m spdatalab process-bbox \
    --input dataset.json \
    --use-partitioning \
    --use-parallel \
    --max-workers 4

# 并行处理的完整参数配置
python -m spdatalab process-bbox \
    --input dataset.json \
    --use-partitioning \
    --use-parallel \
    --max-workers 6 \
    --batch 2000 \
    --insert-batch 1000 \
    --create-unified-view \
    --work-dir ./parallel_logs

# 传统顺序处理（对比）
python -m spdatalab process-bbox \
    --input dataset.json \
    --use-partitioning
```

#### 进度跟踪和恢复
```bash
# 指定工作目录（存储进度文件）
python src/spdatalab/dataset/bbox.py \
  --input output/large_dataset.json \
  --batch 1000 \
  --work-dir ./logs/bbox_import_20231201

# 程序中断后，重新运行相同命令即可自动续传
python src/spdatalab/dataset/bbox.py \
  --input output/large_dataset.json \
  --batch 1000 \
  --work-dir ./logs/bbox_import_20231201

# 只重试失败的数据
python src/spdatalab/dataset/bbox.py \
  --input output/large_dataset.json \
  --retry-failed \
  --work-dir ./logs/bbox_import_20231201

# 查看处理统计信息
python src/spdatalab/dataset/bbox.py \
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

```python
from spdatalab.dataset.dataset_manager import DatasetManager
from spdatalab.dataset.bbox import run as bbox_run

# 1. 使用DatasetManager构建数据集
manager = DatasetManager()
dataset = manager.build_dataset_from_index("data/index.txt", "my_dataset")

# 2. 保存数据集
manager.save_dataset(dataset, "output/dataset.json", format='json')

# 3. 处理边界框（基本用法）
bbox_run(
    input_path="output/dataset.json",
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

## 完整工作流程

### 方案1：大规模数据处理（推荐）

```python
# 步骤1：构建数据集
manager = DatasetManager()
dataset = manager.build_dataset_from_index(
    index_file="data/training_index.txt",
    dataset_name="training_dataset_v1",
    description="训练数据集v1 - 400万场景"
)

# 步骤2：保存为Parquet格式（推荐大数据集）
manager.save_dataset(dataset, "output/training_dataset.parquet", format='parquet')

# 步骤3：生成边界框数据（带进度跟踪）
bbox_run(
    input_path="output/training_dataset.parquet",
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
# 查看处理统计
bbox_run(
    input_path="output/dataset.parquet",
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
    input_path="output/dataset.parquet",
    retry_failed=True,
    work_dir="./logs/import",
    batch=500  # 减小批次大小，提高成功率
)
```

### 方案3：JSON格式工作流程

```python
# 适合中小型数据集或需要人类可读格式
manager = DatasetManager()
dataset = manager.build_dataset_from_index(
    index_file="data/validation_index.txt",
    dataset_name="validation_dataset_v1",
    description="验证数据集v1"
)

# 保存为JSON格式
manager.save_dataset(dataset, "output/validation_dataset.json", format='json')

# 生成边界框数据
bbox_run(
    input_path="output/validation_dataset.json",
    batch=500,          
    insert_batch=500,
    work_dir="./logs/validation_import"
)
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
python bbox.py --input dataset.json --work-dir "./logs/imports/$(date +%Y%m%d_%H%M%S)"
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
   python bbox.py --input dataset.json --show-stats --work-dir ./logs
   ```

2. **检查失败记录**：
   ```python
   import pandas as pd
   failed_df = pd.read_parquet("./logs/failed_tokens.parquet")
   print(failed_df['step'].value_counts())
   ```

3. **重试失败数据**：
   ```bash
   python bbox.py --input dataset.json --retry-failed --work-dir ./logs --batch 500
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

## 示例脚本

完整的示例脚本位于 `examples/bbox_usage_example.py`，包含：

- JSON格式完整工作流程
- Parquet格式完整工作流程
- 失败恢复处理示例
- 性能优化配置

运行示例：

```bash
# JSON工作流程
python examples/bbox_usage_example.py --mode json

# Parquet工作流程（推荐）
python examples/bbox_usage_example.py --mode parquet

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

✅ **多进程并行处理** (ProcessPoolExecutor)  
✅ **自动CPU核心数检测**  
✅ **手动worker数量控制**  
✅ **独立进度跟踪**  
✅ **性能提升监控**  
✅ **智能错误处理**  

### 预期性能提升

- **大数据集处理速度提升 2-6倍**
- **CPU资源充分利用**
- **总体处理时间显著减少**

### 相关文件

- **性能测试**: `python test_parallel_performance.py`
- **开发计划**: `DEVELOPMENT_PLAN.md`
- **核心实现**: `src/spdatalab/dataset/bbox.py`
- **演示脚本**: `demo_parallel_commands.py` 