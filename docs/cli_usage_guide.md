# SPDataLab CLI 使用指南

本指南介绍如何使用 `spdatalab` 的命令行接口（CLI）进行数据集管理和边界框处理。

## 🚀 快速开始

### 安装
```bash
pip install -e .  # 从源码安装
# 或者
pip install spdatalab  # 从PyPI安装（如果可用）
```

### 基本命令结构
```bash
python -m spdatalab.cli <command> [options]
```

## 📋 可用命令

### 1. `build-dataset` - 构建数据集

从索引文件构建数据集结构。

```bash
python -m spdatalab.cli build-dataset \
  --index-file data/train_index.txt \
  --dataset-name "training_v1" \
  --description "训练数据集版本1" \
  --output output/train_dataset.json \
  --format json
```

**参数说明：**
- `--index-file`: 索引文件路径，每行格式为 `obs_path@duplicateN`
- `--dataset-name`: 数据集名称
- `--description`: 数据集描述（可选）
- `--output`: 输出文件路径
- `--format`: 输出格式，`json` 或 `parquet`

### 2. `process-bbox` - 处理边界框 ⭐

从数据集文件中提取场景ID并生成边界框数据，支持智能进度跟踪和失败恢复。

#### 基本用法
```bash
python -m spdatalab.cli process-bbox \
  --input output/train_dataset.json \
  --batch 1000 \
  --insert-batch 500 \
  --work-dir ./logs/bbox_import
```

#### 进度跟踪和恢复
```bash
# 大型数据集处理（推荐）
python -m spdatalab.cli process-bbox \
  --input output/large_dataset.parquet \
  --batch 1000 \
  --insert-batch 1000 \
  --work-dir ./logs/large_import_20231201

# 程序中断后，重新运行相同命令即可自动续传
python -m spdatalab.cli process-bbox \
  --input output/large_dataset.parquet \
  --batch 1000 \
  --work-dir ./logs/large_import_20231201

# 只重试失败的数据
python -m spdatalab.cli process-bbox \
  --input output/large_dataset.parquet \
  --retry-failed \
  --work-dir ./logs/large_import_20231201

# 查看处理统计信息
python -m spdatalab.cli process-bbox \
  --input output/large_dataset.parquet \
  --show-stats \
  --work-dir ./logs/large_import_20231201
```

**参数说明：**

*基本参数：*
- `--input`: 输入文件路径（支持JSON/Parquet/文本格式）
- `--batch`: 处理批次大小（每批从数据库获取多少个场景）
- `--insert-batch`: 插入批次大小（每批向数据库插入多少条记录）
- `--create-table`: 是否创建表（如果不存在），默认启用

*进度跟踪参数：*
- `--work-dir`: 工作目录，用于存储进度文件（默认：`./bbox_import_logs`）
- `--retry-failed`: 只重试失败的数据，跳过已成功处理的数据
- `--show-stats`: 显示处理统计信息并退出，不执行实际处理

*性能优化参数（如果使用原版带缓冲区功能）：*
- `--buffer-meters`: 缓冲区大小（米），用于点数据的边界框扩展
- `--precise-buffer`: 使用精确的米级缓冲区（通过投影转换实现）

#### 工作目录结构
```
work_dir/
├── successful_tokens.parquet  # 成功处理的场景ID（高效查询）
├── failed_tokens.parquet      # 失败记录详情（错误分析）
└── progress.json              # 总体进度信息（人类可读）
```

#### 使用场景示例

**场景1：处理大型数据集**
```bash
# 400万场景的数据集处理
python -m spdatalab.cli process-bbox \
  --input output/huge_dataset.parquet \
  --batch 1000 \
  --insert-batch 1000 \
  --work-dir ./logs/huge_import_$(date +%Y%m%d)
```

**场景2：失败恢复**
```bash
# 查看上次处理的结果
python -m spdatalab.cli process-bbox \
  --input output/dataset.json \
  --show-stats \
  --work-dir ./logs/previous_import

# 重试失败的数据
python -m spdatalab.cli process-bbox \
  --input output/dataset.json \
  --retry-failed \
  --batch 500 \
  --work-dir ./logs/previous_import
```

**场景3：分阶段处理**
```bash
# 阶段1：快速处理主要数据
python -m spdatalab.cli process-bbox \
  --input output/dataset.json \
  --batch 2000 \
  --work-dir ./logs/phase1

# 阶段2：处理剩余失败数据
python -m spdatalab.cli process-bbox \
  --input output/dataset.json \
  --retry-failed \
  --batch 500 \
  --work-dir ./logs/phase1
```

### 3. `build-dataset-with-bbox` - 一键式完整工作流程 ⭐

构建数据集并自动处理边界框，提供最便捷的使用方式。

```bash
python -m spdatalab.cli build-dataset-with-bbox \
  --index-file data/train_index.txt \
  --dataset-name "complete_training_v1" \
  --description "完整训练数据集" \
  --output output/complete_dataset.json \
  --format json \
  --batch 1000 \
  --insert-batch 500 \
  --buffer-meters 50 \
  --precise-buffer
```

**跳过边界框处理：**
```bash
# 只构建数据集，不处理边界框
python -m spdatalab.cli build-dataset-with-bbox \
  --index-file data/test_index.txt \
  --dataset-name "test_dataset" \
  --output output/test_dataset.json \
  --skip-bbox
```

### 4. `dataset-info` - 查看数据集信息

显示数据集的详细统计信息。

```bash
python -m spdatalab.cli dataset-info \
  --dataset-file output/train_dataset.json
```

### 5. `list-scenes` - 列出场景ID

列出数据集中的场景ID。

```bash
# 输出到控制台
python -m spdatalab.cli list-scenes \
  --dataset-file output/train_dataset.json

# 保存到文件
python -m spdatalab.cli list-scenes \
  --dataset-file output/train_dataset.json \
  --output output/scene_ids.txt

# 只列出特定子数据集的场景
python -m spdatalab.cli list-scenes \
  --dataset-file output/train_dataset.json \
  --subdataset "lane_change_1"
```

### 6. `generate-scene-ids` - 生成包含倍增的场景ID

生成包含倍增因子的完整场景ID列表。

```bash
python -m spdatalab.cli generate-scene-ids \
  --dataset-file output/train_dataset.json \
  --output output/scene_ids_with_duplicates.txt
```

## 🔧 高级用法

### 进度跟踪和失败恢复

#### 大规模数据处理（400万+场景）
```bash
# 处理大型Parquet数据集（推荐）
python -m spdatalab.cli process-bbox \
  --input output/huge_dataset.parquet \
  --batch 1000 \
  --insert-batch 1000 \
  --work-dir ./logs/production_import_$(date +%Y%m%d_%H%M%S)
```

#### 断点续传和恢复
```bash
# 1. 开始处理大型数据集
python -m spdatalab.cli process-bbox \
  --input output/dataset.parquet \
  --batch 1000 \
  --work-dir ./logs/import_job_001

# 2. 如果程序中断，重新运行相同命令即可续传
python -m spdatalab.cli process-bbox \
  --input output/dataset.parquet \
  --batch 1000 \
  --work-dir ./logs/import_job_001

# 3. 查看处理统计和进度
python -m spdatalab.cli process-bbox \
  --input output/dataset.parquet \
  --show-stats \
  --work-dir ./logs/import_job_001

# 4. 重试失败的数据
python -m spdatalab.cli process-bbox \
  --input output/dataset.parquet \
  --retry-failed \
  --batch 500 \
  --work-dir ./logs/import_job_001
```

#### 工作目录管理
```bash
# 为不同任务创建独立的工作目录
mkdir -p logs/imports/{training,validation,test}

# 训练数据处理
python -m spdatalab.cli process-bbox \
  --input output/train_dataset.parquet \
  --work-dir ./logs/imports/training \
  --batch 1500

# 验证数据处理
python -m spdatalab.cli process-bbox \
  --input output/val_dataset.parquet \
  --work-dir ./logs/imports/validation \
  --batch 1000
```

### 性能调优

#### 系统资源配置建议

**高性能服务器（32GB+ 内存）**
```bash
python -m spdatalab.cli process-bbox \
  --input dataset.parquet \
  --batch 2000 \
  --insert-batch 2000 \
  --work-dir ./logs/high_perf
```

**标准配置（16GB 内存）**
```bash
python -m spdatalab.cli process-bbox \
  --input dataset.parquet \
  --batch 1000 \
  --insert-batch 1000 \
  --work-dir ./logs/standard
```

**资源受限（8GB 内存）**
```bash
python -m spdatalab.cli process-bbox \
  --input dataset.parquet \
  --batch 500 \
  --insert-batch 500 \
  --work-dir ./logs/limited
```

#### 数据集格式选择

**大型数据集（100万+场景）** - 推荐Parquet格式：
```bash
# 构建时直接使用Parquet
python -m spdatalab.cli build-dataset \
  --index-file large_index.txt \
  --dataset-name "large_dataset" \
  --output output/large_dataset.parquet \
  --format parquet

# 处理时性能更佳
python -m spdatalab.cli process-bbox \
  --input output/large_dataset.parquet \
  --batch 1500 \
  --work-dir ./logs/large_import
```

**中小型数据集（<50万场景）** - JSON格式便于调试：
```bash
python -m spdatalab.cli build-dataset \
  --index-file small_index.txt \
  --dataset-name "small_dataset" \
  --output output/small_dataset.json \
  --format json
```

### 并发和分片处理

#### 数据集分片处理
```bash
# 对于超大数据集，可以考虑分片处理
# 1. 按子数据集分割索引文件
split -l 1000 large_index.txt index_part_

# 2. 分别处理各个分片
for part in index_part_*; do
    python -m spdatalab.cli build-dataset-with-bbox \
        --index-file "$part" \
        --dataset-name "dataset_$(basename $part)" \
        --output "output/dataset_$(basename $part).parquet" \
        --format parquet \
        --batch 1000 \
        --work-dir "./logs/$(basename $part)"
done
```

### 监控和调试

#### 实时监控处理进度
```bash
# 在另一个终端监控进度文件
watch -n 30 'cat ./logs/import/progress.json | jq .'

# 或者定期查看统计信息
while true; do
    python -m spdatalab.cli process-bbox \
        --input dataset.json \
        --show-stats \
        --work-dir ./logs/import
    sleep 300  # 每5分钟检查一次
done
```

#### 故障排查
```bash
# 查看失败记录的详细信息
python -c "
import pandas as pd
df = pd.read_parquet('./logs/import/failed_tokens.parquet')
print('失败步骤统计:')
print(df['step'].value_counts())
print('\n最近失败记录:')
print(df.tail(10)[['scene_token', 'step', 'error_msg', 'failed_at']])
"
```

### 缓冲区配置（如果使用带缓冲区的版本）

#### 快速模式（默认）
适用于大多数场景，使用度数近似转换：
```bash
python -m spdatalab.cli process-bbox \
  --input dataset.json \
  --buffer-meters 50 \
  --work-dir ./logs/quick_mode
```

#### 精确模式
使用投影坐标系进行精确的米级缓冲区计算：
```bash
python -m spdatalab.cli process-bbox \
  --input dataset.json \
  --buffer-meters 100 \
  --precise-buffer \
  --work-dir ./logs/precise_mode
```

## 📂 文件格式支持

### 输入格式

#### 索引文件 (*.txt)
```
obs://bucket/path1/file1.jsonl@duplicate10
obs://bucket/path2/file2.jsonl@duplicate5
obs://bucket/path3/file3.jsonl@duplicate20
```

#### 数据集文件
- **JSON格式**: `.json` - 人类可读，便于调试
- **Parquet格式**: `.parquet` - 高性能，适合大数据集

#### 场景ID文件 (*.txt)
```
scene_id_001
scene_id_002
scene_id_003
```

### 输出格式

#### JSON数据集文件示例
```json
{
  "name": "training_dataset_v1",
  "description": "训练数据集版本1",
  "created_at": "2025-01-18T10:30:00",
  "subdatasets": [
    {
      "name": "example",
      "obs_path": "obs://bucket/path/file.jsonl",
      "duplication_factor": 10,
      "scene_count": 100,
      "scene_ids": ["scene_001", "scene_002", "..."]
    }
  ],
  "total_scenes": 1000,
  "total_unique_scenes": 100
}
```

## 🔄 常用工作流程

### 1. 快速开始（小型数据集）
```bash
# 一键完成所有操作
python -m spdatalab.cli build-dataset-with-bbox \
  --index-file data/index.txt \
  --dataset-name "my_dataset" \
  --output output/my_dataset.json \
  --work-dir ./logs/quick_start
```

### 2. 大规模数据处理（推荐）
```bash
# 步骤1：构建Parquet格式数据集
python -m spdatalab.cli build-dataset \
  --index-file data/large_index.txt \
  --dataset-name "large_dataset_v1" \
  --description "大规模训练数据集" \
  --output output/large_dataset.parquet \
  --format parquet

# 步骤2：查看数据集统计
python -m spdatalab.cli dataset-info \
  --dataset-file output/large_dataset.parquet

# 步骤3：处理边界框（带进度跟踪）
python -m spdatalab.cli process-bbox \
  --input output/large_dataset.parquet \
  --batch 1000 \
  --insert-batch 1000 \
  --work-dir ./logs/large_import_$(date +%Y%m%d)

# 步骤4：如果中断，重新运行继续处理
python -m spdatalab.cli process-bbox \
  --input output/large_dataset.parquet \
  --batch 1000 \
  --work-dir ./logs/large_import_20231201

# 步骤5：查看处理结果
python -m spdatalab.cli process-bbox \
  --input output/large_dataset.parquet \
  --show-stats \
  --work-dir ./logs/large_import_20231201
```

### 3. 生产环境处理流程
```bash
# 创建工作目录
mkdir -p ./logs/production/$(date +%Y%m%d)
WORK_DIR="./logs/production/$(date +%Y%m%d)"

# 第一阶段：快速处理主要数据
python -m spdatalab.cli process-bbox \
  --input output/production_dataset.parquet \
  --batch 1500 \
  --insert-batch 1500 \
  --work-dir "$WORK_DIR"

# 第二阶段：检查处理结果
python -m spdatalab.cli process-bbox \
  --input output/production_dataset.parquet \
  --show-stats \
  --work-dir "$WORK_DIR"

# 第三阶段：重试失败数据
python -m spdatalab.cli process-bbox \
  --input output/production_dataset.parquet \
  --retry-failed \
  --batch 500 \
  --work-dir "$WORK_DIR"

# 第四阶段：最终验证
python -m spdatalab.cli process-bbox \
  --input output/production_dataset.parquet \
  --show-stats \
  --work-dir "$WORK_DIR"
```

### 4. 分步操作（调试和开发）
```bash
# 步骤1：构建数据集
python -m spdatalab.cli build-dataset \
  --index-file data/debug_index.txt \
  --dataset-name "debug_dataset" \
  --output output/debug_dataset.json

# 步骤2：查看数据集信息
python -m spdatalab.cli dataset-info \
  --dataset-file output/debug_dataset.json

# 步骤3：导出场景ID进行验证
python -m spdatalab.cli list-scenes \
  --dataset-file output/debug_dataset.json \
  --output output/debug_scene_ids.txt

# 步骤4：小批量测试边界框处理
python -m spdatalab.cli process-bbox \
  --input output/debug_dataset.json \
  --batch 100 \
  --insert-batch 50 \
  --work-dir ./logs/debug

# 步骤5：查看测试结果
python -m spdatalab.cli process-bbox \
  --input output/debug_dataset.json \
  --show-stats \
  --work-dir ./logs/debug
```

### 5. 多数据集并行处理
```bash
# 并行处理多个数据集
datasets=("train" "val" "test")

for dataset in "${datasets[@]}"; do
    echo "处理 ${dataset} 数据集..."
    
    # 每个数据集使用独立的工作目录
    python -m spdatalab.cli process-bbox \
        --input "output/${dataset}_dataset.parquet" \
        --batch 1000 \
        --work-dir "./logs/${dataset}_import" &
done

# 等待所有后台任务完成
wait

# 检查所有数据集的处理结果
for dataset in "${datasets[@]}"; do
    echo "=== ${dataset} 数据集处理结果 ==="
    python -m spdatalab.cli process-bbox \
        --input "output/${dataset}_dataset.parquet" \
        --show-stats \
        --work-dir "./logs/${dataset}_import"
done
```

## 🐛 故障排除

### 常见错误和解决方案

#### 1. 数据库连接失败
```
错误: 连接数据库失败
ERROR: could not connect to server
```
**解决方案:**
```bash
# 检查数据库服务状态
pg_isready -h localhost -p 5432

# 检查连接参数
export LOCAL_DSN="postgresql+psycopg://username:password@host:port/database"

# 测试连接
python -c "from sqlalchemy import create_engine; create_engine('$LOCAL_DSN').connect()"
```

#### 2. 进度跟踪文件问题
```
错误: 加载成功记录失败: FileNotFoundError
警告: 未安装pyarrow，将使用降级的文本文件模式
```
**解决方案:**
```bash
# 安装依赖
pip install pandas pyarrow

# 检查工作目录权限
ls -la ./logs/bbox_import/
chmod 755 ./logs/bbox_import/

# 手动清理损坏的状态文件
rm -rf ./logs/bbox_import/*.parquet
```

#### 3. 内存和性能问题
```
错误: MemoryError
错误: 处理速度过慢
```
**解决方案:**
```bash
# 减小批次大小
python -m spdatalab.cli process-bbox \
  --input dataset.parquet \
  --batch 500 \
  --insert-batch 200 \
  --work-dir ./logs/low_memory

# 使用系统监控
htop  # 监控内存使用
iotop # 监控磁盘I/O
```

#### 4. 重复数据和约束冲突
```
错误: UNIQUE constraint failed
错误: duplicate key value violates unique constraint
```
**解决方案:**
```bash
# 程序会自动处理重复数据，如果仍有问题：
# 1. 清理进度文件重新开始
rm -rf ./logs/import/*

# 2. 或者继续处理（程序会跳过已存在的数据）
python -m spdatalab.cli process-bbox \
  --input dataset.parquet \
  --work-dir ./logs/import
```

#### 5. 索引文件格式错误
```
错误: 解析索引行失败: invalid format
ValueError: not enough values to unpack
```
**解决方案:**
```bash
# 检查索引文件格式
head -5 data/index.txt
# 应该是: obs://path/file.jsonl@duplicate10

# 修复格式
sed 's/@duplicate/@duplicate/g' data/index.txt > data/index_fixed.txt
```

### 进度跟踪问题排查

#### 查看详细状态信息
```bash
# 检查工作目录结构
ls -la ./logs/import/
# 应该包含: successful_tokens.parquet, failed_tokens.parquet, progress.json

# 查看成功记录统计
python -c "
import pandas as pd
try:
    df = pd.read_parquet('./logs/import/successful_tokens.parquet')
    print(f'成功处理: {len(df)} 条记录')
    print(f'最新处理时间: {df["processed_at"].max()}')
except Exception as e:
    print(f'无法读取成功记录: {e}')
"

# 查看失败记录分析
python -c "
import pandas as pd
try:
    df = pd.read_parquet('./logs/import/failed_tokens.parquet')
    print(f'失败记录: {len(df)} 条')
    print('失败步骤统计:')
    print(df['step'].value_counts())
    print('\n最近失败记录:')
    print(df.tail(5)[['scene_token', 'step', 'error_msg']])
except Exception as e:
    print(f'无法读取失败记录: {e}')
"
```

#### 手动恢复和清理
```bash
# 备份当前进度
cp -r ./logs/import ./logs/import_backup_$(date +%Y%m%d_%H%M%S)

# 清理特定步骤的失败记录
python -c "
import pandas as pd
df = pd.read_parquet('./logs/import/failed_tokens.parquet')
# 移除特定类型的失败记录（如临时网络问题）
cleaned = df[df['step'] != 'database_insert']
cleaned.to_parquet('./logs/import/failed_tokens.parquet', index=False)
print(f'清理后剩余失败记录: {len(cleaned)}')
"

# 重置进度（慎用）
# rm -rf ./logs/import/*
```

### 性能优化建议

#### 系统级优化
```bash
# 调整PostgreSQL配置（需要DBA权限）
# shared_buffers = 256MB
# effective_cache_size = 1GB
# work_mem = 4MB

# 监控数据库性能
# SELECT * FROM pg_stat_activity WHERE state = 'active';

# 检查磁盘空间
df -h /var/lib/postgresql/
df -h ./logs/
```

#### 应用级优化
```bash
# 针对不同场景的优化配置

# 高吞吐量处理（SSD + 高内存）
python -m spdatalab.cli process-bbox \
  --input dataset.parquet \
  --batch 2000 \
  --insert-batch 2000 \
  --work-dir ./logs/high_throughput

# 稳定性优先（机械硬盘 + 低内存）
python -m spdatalab.cli process-bbox \
  --input dataset.parquet \
  --batch 200 \
  --insert-batch 100 \
  --work-dir ./logs/stable

# 平衡配置（标准服务器）
python -m spdatalab.cli process-bbox \
  --input dataset.parquet \
  --batch 1000 \
  --insert-batch 500 \
  --work-dir ./logs/balanced
```

### 调试模式

```bash
# 启用详细日志
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"
python -c "import logging; logging.basicConfig(level=logging.DEBUG)"

# 使用调试参数
python -m spdatalab.cli process-bbox \
  --input dataset.json \
  --batch 10 \
  --insert-batch 5 \
  --work-dir ./logs/debug \
  2>&1 | tee debug.log
```

## 📊 性能基准和建议

### 批次大小调优

| 系统配置 | 推荐batch | 推荐insert_batch | 适用场景 |
|----------|-----------|------------------|----------|
| 32GB+ RAM, SSD | 2000 | 2000 | 生产环境，高性能 |
| 16GB RAM, SSD | 1000 | 1000 | 标准配置 |
| 8GB RAM, HDD | 500 | 500 | 开发环境 |
| 4GB RAM, HDD | 200 | 200 | 资源受限 |

### 数据集大小建议

| 场景规模 | 推荐格式 | 推荐工作流程 | 预计处理时间 |
|----------|----------|--------------|-------------|
| <10万场景 | JSON | 分步操作 | <1小时 |
| 10-100万场景 | Parquet | 大规模处理 | 1-5小时 |
| 100-500万场景 | Parquet | 生产环境流程 | 5-20小时 |
| >500万场景 | Parquet | 分片并行处理 | >20小时 |

## 💡 最佳实践

1. **首次使用**：使用 `build-dataset-with-bbox` 命令进行快速上手
2. **大数据集**：优先使用Parquet格式和较大的批次大小
3. **调试**：先用小数据集测试，确认配置无误后再处理完整数据
4. **监控**：观察内存使用和处理速度，适当调整批次大小
5. **备份**：处理重要数据前先备份原始索引文件

## 🔗 相关资源

- [BBox集成指南](bbox_integration_guide.md)
- [数据集管理器文档](dataset_manager_guide.md)
- [API参考文档](api_reference.md) 