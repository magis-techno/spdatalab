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

### 2. `process-bbox` - 处理边界框

从数据集文件中提取场景ID并生成边界框数据。

```bash
python -m spdatalab.cli process-bbox \
  --input output/train_dataset.json \
  --batch 1000 \
  --insert-batch 500 \
  --buffer-meters 50 \
  --precise-buffer
```

**参数说明：**
- `--input`: 输入文件路径（支持JSON/Parquet/文本格式）
- `--batch`: 处理批次大小（每批从数据库获取多少个场景）
- `--insert-batch`: 插入批次大小（每批向数据库插入多少条记录）
- `--buffer-meters`: 缓冲区大小（米），用于点数据的边界框扩展
- `--precise-buffer`: 使用精确的米级缓冲区（通过投影转换实现）

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
  --subdataset "GOD_E2E_golden_lane_change_1"
```

### 6. `generate-scene-ids` - 生成包含倍增的场景ID

生成包含倍增因子的完整场景ID列表。

```bash
python -m spdatalab.cli generate-scene-ids \
  --dataset-file output/train_dataset.json \
  --output output/scene_ids_with_duplicates.txt
```

## 🔧 高级用法

### 缓冲区配置

#### 快速模式（默认）
适用于大多数场景，使用度数近似转换：
```bash
python -m spdatalab.cli process-bbox \
  --input dataset.json \
  --buffer-meters 50  # 约50米缓冲区
```

#### 精确模式
使用投影坐标系进行精确的米级缓冲区计算：
```bash
python -m spdatalab.cli process-bbox \
  --input dataset.json \
  --buffer-meters 100 \
  --precise-buffer  # 精确100米缓冲区
```

### 性能调优

#### 高性能配置（适合大型数据集）
```bash
python -m spdatalab.cli build-dataset-with-bbox \
  --index-file large_index.txt \
  --dataset-name "large_dataset" \
  --output output/large_dataset.parquet \
  --format parquet \
  --batch 2000 \
  --insert-batch 1000 \
  --buffer-meters 50
```

#### 资源受限配置
```bash
python -m spdatalab.cli build-dataset-with-bbox \
  --index-file small_index.txt \
  --dataset-name "small_dataset" \
  --output output/small_dataset.json \
  --batch 500 \
  --insert-batch 200 \
  --buffer-meters 30
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
      "name": "GOD_E2E_example",
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

### 1. 快速开始（推荐）
```bash
# 一键完成所有操作
python -m spdatalab.cli build-dataset-with-bbox \
  --index-file data/index.txt \
  --dataset-name "my_dataset" \
  --output output/my_dataset.json \
  --buffer-meters 50
```

### 2. 分步操作
```bash
# 步骤1：构建数据集
python -m spdatalab.cli build-dataset \
  --index-file data/index.txt \
  --dataset-name "my_dataset" \
  --output output/my_dataset.json

# 步骤2：查看数据集信息
python -m spdatalab.cli dataset-info \
  --dataset-file output/my_dataset.json

# 步骤3：处理边界框
python -m spdatalab.cli process-bbox \
  --input output/my_dataset.json \
  --buffer-meters 50
```

### 3. 数据集操作
```bash
# 查看数据集统计
python -m spdatalab.cli dataset-info \
  --dataset-file output/my_dataset.json

# 导出场景ID
python -m spdatalab.cli list-scenes \
  --dataset-file output/my_dataset.json \
  --output output/scene_ids.txt

# 生成包含倍增的场景列表
python -m spdatalab.cli generate-scene-ids \
  --dataset-file output/my_dataset.json \
  --output output/duplicated_scene_ids.txt
```

## 🐛 故障排除

### 常见错误

#### 1. 数据库连接失败
```
错误: 连接数据库失败
```
**解决方案:**
- 检查数据库连接配置
- 确认数据库服务正在运行
- 验证用户权限

#### 2. 索引文件格式错误
```
错误: 解析索引行失败
```
**解决方案:**
- 确保索引文件格式正确: `obs_path@duplicateN`
- 检查文件编码（使用UTF-8）

#### 3. 内存不足
```
错误: MemoryError
```
**解决方案:**
- 减小批次大小: `--batch 500 --insert-batch 200`
- 使用Parquet格式: `--format parquet`

#### 4. 依赖缺失
```
错误: ImportError: 需要安装 pandas 和 pyarrow
```
**解决方案:**
```bash
pip install pandas pyarrow
```

### 调试模式

使用 `-v` 参数启用详细输出：
```bash
python -m spdatalab.cli -v build-dataset-with-bbox \
  --index-file data/index.txt \
  --dataset-name "debug_dataset" \
  --output output/debug_dataset.json
```

## 📊 性能建议

### 批次大小调优

| 系统配置 | 推荐配置 |
|----------|----------|
| **高性能服务器** | `--batch 2000 --insert-batch 1000` |
| **标准配置** | `--batch 1000 --insert-batch 500` |
| **资源受限** | `--batch 500 --insert-batch 200` |

### 格式选择

| 使用场景 | 推荐格式 | 原因 |
|----------|----------|------|
| **开发调试** | JSON | 人类可读，便于检查 |
| **生产环境** | Parquet | 高性能，压缩率高 |
| **大数据集** | Parquet | 内存效率更高 |

### 缓冲区选择

| 精度需求 | 推荐模式 | 性能 |
|----------|----------|------|
| **高精度** | `--precise-buffer` | 较慢 |
| **一般精度** | 快速模式（默认） | 较快 |

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