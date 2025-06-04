# BBox模块集成指南

本指南介绍如何使用更新后的 `bbox.py` 模块处理 `dataset_manager` 的输出，生成边界框数据。

## 概述

更新后的 `bbox.py` 模块支持以下功能：

1. **多格式输入支持**：
   - JSON格式（dataset_manager的JSON输出）
   - Parquet格式（dataset_manager的Parquet输出）
   - 文本格式（向后兼容原有格式）

2. **性能优化**：
   - 批量数据库插入
   - 错误处理和恢复机制
   - 详细的进度报告

3. **智能格式检测**：
   - 自动识别输入文件格式
   - 无需手动指定格式

## 使用方法

### 命令行使用

```bash
# 处理JSON格式的数据集文件
python src/spdatalab/dataset/bbox.py --input output/dataset.json --batch 1000 --insert-batch 500

# 处理Parquet格式的数据集文件
python src/spdatalab/dataset/bbox.py --input output/dataset.parquet --batch 2000 --insert-batch 1000

# 处理文本格式的场景ID列表（向后兼容）
python src/spdatalab/dataset/bbox.py --input data/scene_ids.txt --batch 500
```

### 参数说明

- `--input`: 输入文件路径，支持JSON/Parquet/文本格式
- `--batch`: 处理批次大小，默认1000（每批从数据库获取多少个场景的信息）
- `--insert-batch`: 插入批次大小，默认1000（每批向数据库插入多少条记录）

### 编程方式使用

```python
from spdatalab.dataset.dataset_manager import DatasetManager
from spdatalab.dataset.bbox import run as bbox_run

# 1. 使用DatasetManager构建数据集
manager = DatasetManager()
dataset = manager.build_dataset_from_index("data/index.txt", "my_dataset")

# 2. 保存数据集
manager.save_dataset(dataset, "output/dataset.json", format='json')

# 3. 处理边界框
bbox_run(
    input_path="output/dataset.json",
    batch=1000,
    insert_batch=500
)
```

## 完整工作流程

### 方案1：JSON格式工作流程

```python
# 步骤1：构建数据集
manager = DatasetManager()
dataset = manager.build_dataset_from_index(
    index_file="data/training_index.txt",
    dataset_name="training_dataset_v1",
    description="训练数据集v1"
)

# 步骤2：保存为JSON格式
manager.save_dataset(dataset, "output/training_dataset.json", format='json')

# 步骤3：生成边界框数据
bbox_run(
    input_path="output/training_dataset.json",
    batch=500,          # 每批处理500个场景
    insert_batch=500    # 每批插入500条记录
)
```

### 方案2：Parquet格式工作流程

```python
# 步骤1：构建数据集
manager = DatasetManager()
dataset = manager.build_dataset_from_index(
    index_file="data/validation_index.txt",
    dataset_name="validation_dataset_v1",
    description="验证数据集v1"
)

# 步骤2：保存为Parquet格式
manager.save_dataset(dataset, "output/validation_dataset.parquet", format='parquet')

# 步骤3：生成边界框数据
bbox_run(
    input_path="output/validation_dataset.parquet",
    batch=1000,         # 每批处理1000个场景
    insert_batch=1000   # 每批插入1000条记录
)
```

## 性能优化建议

### 1. 批次大小调优

根据您的系统资源调整批次大小：

- **内存充足**：可以增大 `batch` 参数（如1000-2000）
- **数据库性能好**：可以增大 `insert_batch` 参数（如1000-2000）
- **资源受限**：减小批次大小（如500-1000）

### 2. 并发处理

对于大型数据集，可以考虑分片并行处理：

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
dataset = manager.load_dataset("output/large_dataset.json")
parts = split_dataset(dataset, num_parts=4)

for i, part in enumerate(parts):
    part_file = f"output/dataset_part_{i+1}.json"
    manager.save_dataset(part, part_file, format='json')
    
    # 并行处理每个分片
    # 可以使用多进程或分布式处理
```

### 3. 数据库优化

- **索引优化**：确保数据库表有适当的索引
- **连接池**：使用数据库连接池提高连接效率
- **批量插入**：使用 `method='multi'` 参数进行批量插入

## 错误处理

模块包含完善的错误处理机制：

1. **文件格式错误**：自动检测并提示格式问题
2. **数据库连接错误**：提供详细的错误信息
3. **批量插入失败**：自动降级为逐行插入
4. **部分数据缺失**：跳过问题数据，继续处理

## 监控和日志

处理过程中会输出详细的进度信息：

```
开始处理输入文件: output/dataset.json
从JSON文件加载了 15000 个scene_id
开始处理 15000 个场景，批次大小: 1000
[批次 1] 处理 1000 个场景
[批次 1] 获取到 950 条元数据
[批次 1] 获取到 950 条边界框数据
[批次 1] 合并后得到 950 条记录
[批量插入] 已插入: 500/950 行
[批量插入] 已插入: 950/950 行
[批次 1] 完成，插入 950 条记录
[累计进度] 已处理: 950, 已插入: 950
...
处理完成！总计处理: 14250 条记录，成功插入: 14250 条记录
```

## 故障排除

### 常见问题

1. **ImportError: 需要安装 pandas 和 pyarrow**
   ```bash
   pip install pandas pyarrow
   ```

2. **数据库连接失败**
   - 检查数据库连接字符串
   - 确认数据库服务正在运行
   - 验证用户权限

3. **内存不足**
   - 减小批次大小
   - 使用分片处理

4. **插入速度慢**
   - 增大 `insert_batch` 参数
   - 检查数据库索引
   - 考虑使用更快的存储

### 调试模式

可以通过修改日志级别获取更详细的调试信息：

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## 示例脚本

完整的示例脚本位于 `examples/bbox_usage_example.py`，包含：

- JSON格式完整工作流程
- Parquet格式完整工作流程
- 加载已有数据集的处理方式

运行示例：

```bash
# JSON工作流程
python examples/bbox_usage_example.py --mode json

# Parquet工作流程
python examples/bbox_usage_example.py --mode parquet

# 加载已有数据集
python examples/bbox_usage_example.py --mode load
``` 