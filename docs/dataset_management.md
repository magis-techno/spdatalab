# 数据集管理系统

## 概述

数据集管理系统提供了一个结构化的方式来组织和管理包含多个子数据集的大型数据集。系统支持从索引文件构建数据集，提取场景ID，处理数据倍增，以及各种查询和导出功能。

**重要特性**：
- 支持JSON和Parquet两种存储格式
- Parquet格式适合大规模数据（400万+场景ID）
- 高效的压缩和查询性能
- 自动格式检测和转换

## 格式选择指南

### JSON格式
- **适用场景**：小到中型数据集（< 10万场景ID）
- **优势**：可读性好，兼容性强，调试方便
- **劣势**：文件较大，读取速度慢

### Parquet格式 ⭐ 推荐用于大数据集
- **适用场景**：大型数据集（> 10万场景ID，特别是400万+）
- **优势**：
  - 列式存储，压缩率高（通常比JSON小80-90%）
  - 读取速度快
  - 支持高效查询和过滤
  - 内存使用更少
- **劣势**：需要额外依赖（pandas, pyarrow）

## 安装要求

基础功能（JSON格式）：
```bash
# 基础安装，只需要现有依赖
```

Parquet格式支持：
```bash
# 安装parquet格式依赖
pip install pandas pyarrow
```

## 数据结构设计

### 核心概念

1. **数据集 (Dataset)**: 包含多个子数据集的顶层容器
2. **子数据集 (SubDataset)**: 存储在OBS上的实际数据文件，包含多个场景
3. **场景ID (Scene ID)**: 每个场景的唯一标识符
4. **倍增因子 (Duplication Factor)**: 每个子数据集的重复倍数

### 数据结构

```python
@dataclass
class SubDataset:
    name: str                    # 子数据集名称
    obs_path: str               # OBS路径
    duplication_factor: int     # 倍增因子
    scene_count: int           # 场景数量
    scene_ids: List[str]       # 场景ID列表
    metadata: Dict             # 额外元数据

@dataclass
class Dataset:
    name: str                  # 数据集名称
    description: str           # 数据集描述
    subdatasets: List[SubDataset]  # 子数据集列表
    created_at: str           # 创建时间
    total_scenes: int         # 总场景数（含倍增）
    total_unique_scenes: int  # 唯一场景数（不含倍增）
    metadata: Dict           # 额外元数据
```

## 使用方法

### 1. 从索引文件构建数据集

索引文件格式：每行包含一个OBS路径和倍增因子
```
obs://path/to/subdataset1/file.shrink@duplicate20
obs://path/to/subdataset2/file.shrink@duplicate10
obs://path/to/subdataset3/file.shrink@duplicate5
```

使用命令行工具：
```bash
# 构建数据集 - JSON格式（默认）
python -m spdatalab.cli build-dataset \
    --index-file data/index.txt \
    --dataset-name "GOD_E2E_Dataset" \
    --description "GOD E2E training dataset" \
    --output datasets/god_e2e_dataset.json

# 构建数据集 - Parquet格式（推荐用于大数据集）
python -m spdatalab.cli build-dataset \
    --index-file data/index.txt \
    --dataset-name "GOD_E2E_Dataset" \
    --description "GOD E2E training dataset" \
    --output datasets/god_e2e_dataset.parquet \
    --format parquet
```

使用Python API：
```python
from spdatalab.dataset.dataset_manager import DatasetManager

manager = DatasetManager()
dataset = manager.build_dataset_from_index(
    "data/index.txt",
    "GOD_E2E_Dataset", 
    "GOD E2E training dataset"
)

# 保存为JSON格式
manager.save_dataset(dataset, "datasets/god_e2e_dataset.json", format='json')

# 保存为Parquet格式（推荐用于大数据集）
manager.save_dataset(dataset, "datasets/god_e2e_dataset.parquet", format='parquet')
```

### 2. 查看数据集信息

```bash
# 显示数据集详细信息（自动检测格式）
python -m spdatalab.cli dataset-info --dataset-file datasets/god_e2e_dataset.parquet

# 获取统计信息
python -m spdatalab.cli dataset-stats --dataset-file datasets/god_e2e_dataset.parquet
```

输出示例：
```
数据集信息:
  名称: GOD_E2E_Dataset
  描述: GOD E2E training dataset
  创建时间: 2025-01-27T10:30:00
  子数据集数量: 3
  总唯一场景数: 4000000
  总场景数(含倍增): 60000000

子数据集详情:
  1. GOD_E2E_golden_lane_change_1_sub_ddi_2773412e2e_2025_05_18_11_07_59
     - OBS路径: obs://yw-ads-training-gy1/data/god/.../file.shrink
     - 场景数: 2000000
     - 倍增因子: 20
     - 倍增后场景数: 40000000
  ...
```

### 3. 列出场景ID

```bash
# 列出所有场景ID（不含倍增）
python -m spdatalab.cli list-scenes \
    --dataset-file datasets/god_e2e_dataset.parquet \
    --output scene_ids.txt

# 列出特定子数据集的场景ID
python -m spdatalab.cli list-scenes \
    --dataset-file datasets/god_e2e_dataset.parquet \
    --subdataset "GOD_E2E_golden_lane_change_1_sub_ddi_2773412e2e_2025_05_18_11_07_59" \
    --output lane_change_scenes.txt

# 直接在控制台显示
python -m spdatalab.cli list-scenes \
    --dataset-file datasets/god_e2e_dataset.parquet
```

### 4. 导出场景ID为Parquet格式

```bash
# 导出唯一场景ID（不含倍增）
python -m spdatalab.cli export-scene-ids \
    --dataset-file datasets/god_e2e_dataset.parquet \
    --output scene_ids_unique.parquet

# 导出包含倍增的完整场景ID列表
python -m spdatalab.cli export-scene-ids \
    --dataset-file datasets/god_e2e_dataset.parquet \
    --output scene_ids_full.parquet \
    --include-duplicates
```

### 5. 查询Parquet数据集

```bash
# 查询所有数据
python -m spdatalab.cli query-parquet \
    --parquet-file datasets/god_e2e_dataset.parquet

# 按子数据集过滤
python -m spdatalab.cli query-parquet \
    --parquet-file datasets/god_e2e_dataset.parquet \
    --subdataset "GOD_E2E_golden_lane_change_1_sub_ddi_2773412e2e_2025_05_18_11_07_59"

# 按倍增因子过滤
python -m spdatalab.cli query-parquet \
    --parquet-file datasets/god_e2e_dataset.parquet \
    --duplication-factor 20

# 保存查询结果
python -m spdatalab.cli query-parquet \
    --parquet-file datasets/god_e2e_dataset.parquet \
    --subdataset "GOD_E2E_golden_lane_change_1_sub_ddi_2773412e2e_2025_05_18_11_07_59" \
    --output filtered_results.parquet
```

### 6. Python API 使用示例

```python
from spdatalab.dataset.dataset_manager import DatasetManager

# 创建管理器
manager = DatasetManager()

# 加载数据集（自动检测格式）
dataset = manager.load_dataset("datasets/god_e2e_dataset.parquet")

# 获取子数据集信息
subdataset = manager.get_subdataset_info(dataset, "GOD_E2E_golden_lane_change_1_sub_ddi_2773412e2e_2025_05_18_11_07_59")
if subdataset:
    print(f"子数据集: {subdataset.name}")
    print(f"场景数: {subdataset.scene_count}")
    print(f"倍增因子: {subdataset.duplication_factor}")

# 列出所有场景ID
all_scene_ids = manager.list_scene_ids(dataset)
print(f"总场景数: {len(all_scene_ids)}")

# 列出特定子数据集的场景ID
subset_scene_ids = manager.list_scene_ids(dataset, "GOD_E2E_golden_lane_change_1_sub_ddi_2773412e2e_2025_05_18_11_07_59")
print(f"子数据集场景数: {len(subset_scene_ids)}")

# 生成包含倍增的场景ID
duplicated_scenes = list(manager.generate_scene_list_with_duplication(dataset))
print(f"倍增后总场景数: {len(duplicated_scenes)}")

# 查询Parquet数据（仅当数据集为parquet格式时可用）
df = manager.query_scenes_parquet("datasets/god_e2e_dataset.parquet", duplication_factor=20)
print(f"倍增因子为20的场景数: {len(df)}")

# 导出场景ID为Parquet格式
manager.export_scene_ids_parquet(dataset, "scene_ids.parquet", include_duplicates=True)

# 获取统计信息
stats = manager.get_dataset_stats(dataset)
print(f"数据集统计: {stats}")

# 保存修改后的数据集
manager.save_dataset(dataset, "datasets/updated_dataset.parquet", format='parquet')
```

## 数据集文件格式

### JSON格式
```json
{
  "name": "GOD_E2E_Dataset",
  "description": "GOD E2E training dataset",
  "created_at": "2025-01-27T10:30:00.123456",
  "total_scenes": 15000,
  "total_unique_scenes": 1000,
  "metadata": {},
  "subdatasets": [
    {
      "name": "GOD_E2E_golden_lane_change_1_sub_ddi_2773412e2e_2025_05_18_11_07_59",
      "obs_path": "obs://yw-ads-training-gy1/data/god/.../file.shrink",
      "duplication_factor": 20,
      "scene_count": 500,
      "scene_ids": ["scene_001", "scene_002", "..."],
      "metadata": {}
    }
  ]
}
```

### Parquet格式
Parquet格式将数据存储为表格结构，每行代表一个场景ID：

| dataset_name | dataset_description | subdataset_name | obs_path | duplication_factor | scene_id | metadata |
|--------------|---------------------|-----------------|----------|-------------------|----------|----------|
| GOD_E2E_Dataset | GOD E2E training... | GOD_E2E_golden... | obs://... | 20 | scene_001 | {...} |
| GOD_E2E_Dataset | GOD E2E training... | GOD_E2E_golden... | obs://... | 20 | scene_002 | {...} |

同时会生成一个 `.meta.json` 文件保存数据集元信息。

## 性能对比

以下是400万场景ID的数据集的格式对比：

| 格式 | 文件大小 | 加载时间 | 内存使用 | 查询性能 |
|------|----------|----------|----------|----------|
| JSON | ~800MB | ~30s | ~2GB | 慢 |
| Parquet | ~120MB | ~3s | ~400MB | 快 |

**结论**：对于大型数据集，Parquet格式在所有方面都有显著优势。

## 最佳实践

### 1. 格式选择策略
```python
# 根据数据集大小选择格式
def choose_format(scene_count):
    if scene_count < 100000:
        return 'json'  # 小数据集，使用JSON便于调试
    else:
        return 'parquet'  # 大数据集，使用Parquet提升性能
```

### 2. 大数据集处理
- 优先使用Parquet格式
- 利用查询功能进行数据过滤
- 分批处理超大数据集
- 使用合适的倍增因子避免过度重复

### 3. 数据集命名
- 使用描述性的数据集名称
- 包含版本信息或日期
- 添加有意义的描述

### 4. 性能优化
```python
# 对于非常大的数据集，可以分批构建
def build_large_dataset_in_batches(index_files, dataset_name):
    manager = DatasetManager()
    datasets = []
    
    for i, index_file in enumerate(index_files):
        batch_dataset = manager.build_dataset_from_index(
            index_file, 
            f"{dataset_name}_batch_{i}"
        )
        datasets.append(batch_dataset)
    
    # 合并数据集
    merged_dataset = merge_datasets(datasets, dataset_name)
    return merged_dataset
```

### 5. 查询优化
```python
# 使用pandas进行复杂查询
import pandas as pd

def complex_query_example(parquet_file):
    # 直接使用pandas读取和查询
    df = pd.read_parquet(parquet_file)
    
    # 复杂过滤条件
    filtered = df[
        (df['duplication_factor'] >= 10) & 
        (df['subdataset_name'].str.contains('lane_change'))
    ]
    
    # 统计分析
    stats = filtered.groupby('subdataset_name')['scene_id'].count()
    
    return filtered, stats
```

## 故障排除

### 常见问题

1. **Parquet依赖缺失**
   ```bash
   # 安装必要依赖
   pip install pandas pyarrow
   ```

2. **内存不足（处理超大数据集）**
   ```python
   # 使用chunked处理
   def process_large_dataset_chunked(parquet_file, chunk_size=100000):
       for chunk in pd.read_parquet(parquet_file, chunksize=chunk_size):
           # 处理每个chunk
           process_chunk(chunk)
   ```

3. **格式转换**
   ```bash
   # JSON转Parquet
   python -c "
   from spdatalab.dataset.dataset_manager import DatasetManager
   manager = DatasetManager()
   dataset = manager.load_dataset('dataset.json')
   manager.save_dataset(dataset, 'dataset.parquet', format='parquet')
   "
   ```

### 日志配置

系统使用Python logging模块，可以通过设置日志级别获取详细信息：

```python
import logging
logging.basicConfig(level=logging.INFO)
```

或在命令行中查看详细输出。

## 扩展功能

### 1. 自定义查询
```python
def custom_parquet_query(parquet_file, custom_filter):
    """自定义Parquet查询函数。"""
    df = pd.read_parquet(parquet_file)
    
    # 应用自定义过滤器
    filtered_df = df[custom_filter(df)]
    
    return filtered_df

# 使用示例
result = custom_parquet_query(
    "dataset.parquet",
    lambda df: (df['scene_id'].str.startswith('scene_abc')) & 
               (df['duplication_factor'] > 5)
)
```

### 2. 批量操作
```python
def batch_export_by_subdataset(dataset, output_dir):
    """按子数据集批量导出。"""
    manager = DatasetManager()
    
    for subdataset in dataset.subdatasets:
        output_file = Path(output_dir) / f"{subdataset.name}.parquet"
        
        # 创建只包含当前子数据集的临时数据集
        temp_dataset = Dataset(
            name=f"temp_{subdataset.name}",
            subdatasets=[subdataset]
        )
        
        manager.export_scene_ids_parquet(temp_dataset, str(output_file))
```

### 3. 数据验证
```python
def validate_dataset_integrity(parquet_file):
    """验证数据集完整性。"""
    df = pd.read_parquet(parquet_file)
    
    # 检查必要字段
    required_columns = ['dataset_name', 'subdataset_name', 'scene_id', 'duplication_factor']
    missing_columns = set(required_columns) - set(df.columns)
    
    if missing_columns:
        raise ValueError(f"缺少必要字段: {missing_columns}")
    
    # 检查数据完整性
    null_counts = df.isnull().sum()
    if null_counts.any():
        print(f"发现空值: {null_counts[null_counts > 0]}")
    
    # 检查重复scene_id（在同一子数据集内）
    duplicates = df.groupby(['subdataset_name', 'scene_id']).size()
    duplicates = duplicates[duplicates > 1]
    
    if len(duplicates) > 0:
        print(f"发现重复scene_id: {len(duplicates)}个")
    
    return {
        'total_records': len(df),
        'unique_scene_ids': df['scene_id'].nunique(),
        'unique_subdatasets': df['subdataset_name'].nunique(),
        'has_nulls': null_counts.any(),
        'has_duplicates': len(duplicates) > 0
    }
``` 