# 数据集管理系统

## 概述

数据集管理系统提供了一个结构化的方式来组织和管理包含多个子数据集的大型数据集。系统支持从索引文件构建数据集，提取场景ID，处理数据倍增，以及各种查询和导出功能。

**重要特性**：
- 支持JSON和Parquet两种存储格式
- Parquet格式适合大规模数据（400万+场景ID）
- 高效的压缩和查询性能
- 自动格式检测和转换
- 支持多种数据源类型（标准训练数据、问题单数据）

## 数据源类型

### 1. 标准训练数据模式（默认）
- **数据来源**：OBS存储的shrink文件
- **索引格式**：`obs_path@duplicateN`
- **场景提取**：直接从shrink文件中提取scene_id
- **适用场景**：常规训练数据集构建

### 2. 问题单数据模式 🆕
- **数据来源**：问题单系统的URL链接
- **索引格式**：问题单URL或URL+属性
- **场景提取**：通过数据库查询获取scene_id
- **适用场景**：问题单数据分析和处理

#### 问题单数据处理流程
1. **URL解析** → 提取数据名称（如 `10000_ddi-application-667754027299119535`）
2. **第一次查询** → 通过数据名称从 `elasticsearch_ros.ods_ddi_index002_datalake` 获取 `defect_id`
3. **第二次查询** → 通过 `defect_id` 从 `transform.ods_t_data_fragment_datalake` 获取 `scene_id`
4. **数据集构建** → 生成与bbox兼容的数据集格式

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

问题单数据模式要求：
```bash
# 问题单模式需要数据库访问权限
# 确保可以访问以下数据库表：
# - elasticsearch_ros.ods_ddi_index002_datalake
# - transform.ods_t_data_fragment_datalake
# 
# 数据库连接通过 spdatalab.common.io_hive.hive_cursor 建立
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

### 1. 从索引文件构建数据集（标准训练数据）

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
    --dataset-name "Dataset" \
    --description "GOD E2E training dataset" \
    --output datasets/dataset.json

# 构建数据集 - Parquet格式（推荐用于大数据集）
python -m spdatalab.cli build-dataset \
    --index-file data/index.txt \
    --dataset-name "Dataset" \
    --description "GOD E2E training dataset" \
    --output datasets/dataset.parquet \
    --format parquet
```

使用Python API：
```python
from spdatalab.dataset.dataset_manager import DatasetManager

manager = DatasetManager()
dataset = manager.build_dataset_from_index(
    "data/index.txt",
    "Dataset", 
    "GOD E2E training dataset"
)

# 保存为JSON格式
manager.save_dataset(dataset, "datasets/dataset.json", format='json')

# 保存为Parquet格式（推荐用于大数据集）
manager.save_dataset(dataset, "datasets/dataset.parquet", format='parquet')
```

### 2. 从问题单URL构建数据集 🆕

#### 输入文件格式

**基础格式**（当前支持）：
```
https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=10000_ddi-application-667754027299119535
https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=10000_ddi-application-667754027299119536
https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=10000_ddi-application-667754027299119537
```

**扩展格式**（支持额外属性）：
```
https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=10000_ddi-application-667754027299119535|priority=high|region=beijing|type=lane_change
https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=10000_ddi-application-667754027299119536|priority=low|region=shanghai|type=intersection
```

#### 使用命令行工具

```bash
# 构建问题单数据集 - JSON格式
python -m spdatalab.cli build-dataset \
    --index-file defect_urls.txt \
    --dataset-name "DefectDataset" \
    --description "问题单数据集" \
    --output datasets/defect_dataset.json \
    --defect-mode

# 构建问题单数据集 - Parquet格式
python -m spdatalab.cli build-dataset \
    --index-file defect_urls.txt \
    --dataset-name "DefectDataset" \
    --description "问题单数据集" \
    --output datasets/defect_dataset.parquet \
    --format parquet \
    --defect-mode
```

#### 使用Python API

```python
from spdatalab.dataset.dataset_manager import DatasetManager

# 方法1：创建时指定问题单模式
manager = DatasetManager(defect_mode=True)
dataset = manager.build_dataset_from_index(
    "defect_urls.txt",
    "DefectDataset",
    "问题单数据集"
)

# 方法2：运行时指定问题单模式
manager = DatasetManager()
dataset = manager.build_dataset_from_index(
    "defect_urls.txt",
    "DefectDataset",
    "问题单数据集",
    defect_mode=True
)

# 保存数据集
manager.save_dataset(dataset, "datasets/defect_dataset.json", format='json')
```

#### 问题单数据集特点

- **合并架构**：一个文件中的所有URL生成一个子数据集，减少表数量
- **无倍增因子**：问题单数据通常不需要重复，`duplication_factor` 默认为 1
- **场景级属性**：每个场景的属性存储在`scene_attributes`中，支持灵活的自定义字段
- **数据库依赖**：需要访问Hive数据库来查询scene_id
- **错误处理**：详细的统计信息，包括查询失败和无scene_id的情况

#### 输出格式示例

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
            "region": "beijing",
            "type": "lane_change"
          },
          "632c1e86c95a42c9a3b6c83257ed3f83": {
            "original_url": "https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=10000_ddi-application-667754027299119536",
            "data_name": "10000_ddi-application-667754027299119536",
            "line_number": 2,
            "priority": "low",
            "region": "shanghai",
            "type": "intersection"
          },
          "632c1e86c95a42c9a3b6c83257ed3f84": {
            "original_url": "https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=10000_ddi-application-667754027299119537",
            "data_name": "10000_ddi-application-667754027299119537",
            "line_number": 3,
            "priority": "medium",
            "region": "guangzhou"
          }
        }
      }
    }
  ]
}
```

### 3. 查看数据集信息

```bash
# 显示数据集详细信息（自动检测格式）
python -m spdatalab.cli dataset-info --dataset-file datasets/dataset.parquet

# 获取统计信息
python -m spdatalab.cli dataset-stats --dataset-file datasets/dataset.parquet
```

输出示例：
```
数据集信息:
  名称: Dataset
  描述: GOD E2E training dataset
  创建时间: 2025-01-27T10:30:00
  子数据集数量: 3
  总唯一场景数: 4000000
  总场景数(含倍增): 60000000

子数据集详情:
  1. lane_change_1_sub_ddi_2773412e2e_2025_05_18_11_07_59
     - OBS路径: obs://yw-ads-training-gy1/data/god/.../file.shrink
     - 场景数: 2000000
     - 倍增因子: 20
     - 倍增后场景数: 40000000
  ...
```

### 4. 列出场景ID

```bash
# 列出所有场景ID（不含倍增）
python -m spdatalab.cli list-scenes \
    --dataset-file datasets/dataset.parquet \
    --output scene_ids.txt

# 列出特定子数据集的场景ID
python -m spdatalab.cli list-scenes \
    --dataset-file datasets/dataset.parquet \
    --subdataset "lane_change_1_sub_ddi_2773412e2e_2025_05_18_11_07_59" \
    --output lane_change_scenes.txt

# 直接在控制台显示
python -m spdatalab.cli list-scenes \
    --dataset-file datasets/dataset.parquet
```

### 5. 导出场景ID为Parquet格式

```bash
# 导出唯一场景ID（不含倍增）
python -m spdatalab.cli export-scene-ids \
    --dataset-file datasets/dataset.parquet \
    --output scene_ids_unique.parquet

# 导出包含倍增的完整场景ID列表
python -m spdatalab.cli export-scene-ids \
    --dataset-file datasets/dataset.parquet \
    --output scene_ids_full.parquet \
    --include-duplicates
```

### 6. 查询Parquet数据集

```bash
# 查询所有数据
python -m spdatalab.cli query-parquet \
    --parquet-file datasets/dataset.parquet

# 按子数据集过滤
python -m spdatalab.cli query-parquet \
    --parquet-file datasets/dataset.parquet \
    --subdataset "lane_change_1_sub_ddi_2773412e2e_2025_05_18_11_07_59"

# 按倍增因子过滤
python -m spdatalab.cli query-parquet \
    --parquet-file datasets/dataset.parquet \
    --duplication-factor 20

# 保存查询结果
python -m spdatalab.cli query-parquet \
    --parquet-file datasets/dataset.parquet \
    --subdataset "lane_change_1_sub_ddi_2773412e2e_2025_05_18_11_07_59" \
    --output filtered_results.parquet
```

### 7. Python API 使用示例

```python
from spdatalab.dataset.dataset_manager import DatasetManager

# 创建管理器
manager = DatasetManager()

# 加载数据集（自动检测格式）
dataset = manager.load_dataset("datasets/dataset.parquet")

# 获取子数据集信息
subdataset = manager.get_subdataset_info(dataset, "lane_change_1_sub_ddi_2773412e2e_2025_05_18_11_07_59")
if subdataset:
    print(f"子数据集: {subdataset.name}")
    print(f"场景数: {subdataset.scene_count}")
    print(f"倍增因子: {subdataset.duplication_factor}")

# 列出所有场景ID
all_scene_ids = manager.list_scene_ids(dataset)
print(f"总场景数: {len(all_scene_ids)}")

# 列出特定子数据集的场景ID
subset_scene_ids = manager.list_scene_ids(dataset, "lane_change_1_sub_ddi_2773412e2e_2025_05_18_11_07_59")
print(f"子数据集场景数: {len(subset_scene_ids)}")

# 生成包含倍增的场景ID
duplicated_scenes = list(manager.generate_scene_list_with_duplication(dataset))
print(f"倍增后总场景数: {len(duplicated_scenes)}")

# 查询Parquet数据（仅当数据集为parquet格式时可用）
df = manager.query_scenes_parquet("datasets/dataset.parquet", duplication_factor=20)
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
  "name": "Dataset",
  "description": "GOD E2E training dataset",
  "created_at": "2025-01-27T10:30:00.123456",
  "total_scenes": 15000,
  "total_unique_scenes": 1000,
  "metadata": {},
  "subdatasets": [
    {
      "name": "lane_change_1_sub_ddi_2773412e2e_2025_05_18_11_07_59",
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
| Dataset | GOD E2E training... | golden... | obs://... | 20 | scene_001 | {...} |
| Dataset | GOD E2E training... | golden... | obs://... | 20 | scene_002 | {...} |

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
- 问题单数据集建议使用 `defect_` 前缀

### 4. 问题单数据处理
- 确保数据库连接正常
- 监控查询失败率，及时处理异常URL
- 使用扩展属性格式记录问题单的重要信息
- 定期清理和更新问题单数据集
- **推荐合并架构**：一个文件生成一个子数据集，减少bbox表数量
- **场景属性管理**：利用`scene_attributes`存储场景级别的自定义信息

### 5. 性能优化
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

### 6. 查询优化
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

4. **问题单数据库连接失败**
   ```python
   # 检查数据库连接
   from spdatalab.common.io_hive import hive_cursor
   
   try:
       with hive_cursor() as cur:
           cur.execute("SELECT 1")
           print("数据库连接正常")
   except Exception as e:
       print(f"数据库连接失败: {e}")
   ```

5. **问题单URL解析失败**
   ```python
   # 检查URL格式
   url = "https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=10000_ddi-application-667754027299119535"
   
   import re
   pattern = r'dataName=([^&]+)'
   match = re.search(pattern, url)
   if match:
       data_name = match.group(1)
       print(f"提取的数据名称: {data_name}")
   else:
       print("URL格式不正确")
   ```

6. **问题单查询无结果**
   ```python
   # 手动检查数据是否存在
   from spdatalab.common.io_hive import hive_cursor
   
   data_name = "10000_ddi-application-667754027299119535"
   
   with hive_cursor() as cur:
       # 检查第一步查询
       cur.execute(
           "SELECT defect_id FROM elasticsearch_ros.ods_ddi_index002_datalake WHERE id = %s",
           (data_name,)
       )
       result = cur.fetchone()
       if result:
           defect_id = result[0]
           print(f"找到defect_id: {defect_id}")
           
           # 检查第二步查询
           cur.execute(
               "SELECT id FROM transform.ods_t_data_fragment_datalake WHERE origin_source_id = %s",
               (defect_id,)
           )
           result = cur.fetchone()
           if result:
               scene_id = result[0]
               print(f"找到scene_id: {scene_id}")
           else:
               print(f"未找到scene_id，defect_id: {defect_id}")
       else:
           print(f"未找到defect_id，data_name: {data_name}")
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

### 4. 问题单数据处理
```python
def process_defect_urls_with_retry(url_file, max_retries=3):
    """处理问题单URL，支持重试机制。"""
    from spdatalab.dataset.dataset_manager import DatasetManager
    
    manager = DatasetManager(defect_mode=True)
    failed_urls = []
    
    for retry in range(max_retries):
        try:
            dataset = manager.build_dataset_from_index(
                url_file,
                f"DefectDataset_retry_{retry}",
                "问题单数据集"
            )
            
            # 检查失败率
            stats = manager.stats
            fail_rate = stats['failed_files'] / stats['total_files'] if stats['total_files'] > 0 else 0
            
            if fail_rate > 0.1:  # 失败率超过10%
                print(f"警告: 失败率过高 ({fail_rate:.2%})，建议检查数据库连接")
            
            return dataset
            
        except Exception as e:
            print(f"第 {retry + 1} 次尝试失败: {e}")
            if retry == max_retries - 1:
                raise
    
    return None

def analyze_defect_dataset(dataset_file):
    """分析问题单数据集的统计信息。"""
    from spdatalab.dataset.dataset_manager import DatasetManager
    
    manager = DatasetManager()
    dataset = manager.load_dataset(dataset_file)
    
    # 统计问题单属性
    attributes_stats = {}
    
    for subdataset in dataset.subdatasets:
        metadata = subdataset.metadata
        
        # 统计各种属性
        for key, value in metadata.items():
            if key.startswith('data_') or key in ['original_url', 'line_number']:
                continue  # 跳过系统字段
                
            if key not in attributes_stats:
                attributes_stats[key] = {}
            
            if value not in attributes_stats[key]:
                attributes_stats[key][value] = 0
            attributes_stats[key][value] += 1
    
    # 打印统计结果
    print("问题单数据集分析:")
    print(f"总计问题单数量: {len(dataset.subdatasets)}")
    print(f"总计场景数: {dataset.total_scenes}")
    
    for attr, values in attributes_stats.items():
        print(f"\n{attr} 分布:")
        for value, count in sorted(values.items(), key=lambda x: x[1], reverse=True):
            print(f"  {value}: {count}")
    
    return attributes_stats
```

## 问题单数据处理完整示例

### 1. 准备问题单URL文件

创建 `defect_urls.txt` 文件：
```
https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=10000_ddi-application-667754027299119535|priority=high|region=beijing|type=lane_change
https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=10000_ddi-application-667754027299119536|priority=medium|region=shanghai|type=intersection
https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=10000_ddi-application-667754027299119537|priority=low|region=guangzhou|type=merging
```

### 2. 构建问题单数据集

```python
from spdatalab.dataset.dataset_manager import DatasetManager
import logging

# 启用详细日志
logging.basicConfig(level=logging.INFO)

# 创建数据集管理器
manager = DatasetManager(defect_mode=True)

# 构建数据集
dataset = manager.build_dataset_from_index(
    "defect_urls.txt",
    "DefectAnalysisDataset",
    "问题单分析数据集 v1.0"
)

# 保存数据集
manager.save_dataset(dataset, "defect_dataset.json", format='json')
manager.save_dataset(dataset, "defect_dataset.parquet", format='parquet')

print(f"数据集构建完成，包含 {len(dataset.subdatasets)} 个问题单")
```

### 3. 数据集分析

```python
# 加载数据集
dataset = manager.load_dataset("defect_dataset.json")

# 获取统计信息
stats = manager.get_dataset_stats(dataset)
print("数据集统计信息:")
for key, value in stats.items():
    print(f"  {key}: {value}")

# 分析问题单属性
analyze_defect_dataset("defect_dataset.parquet")

# 输出所有场景ID
scene_ids = manager.list_scene_ids(dataset)
print(f"\n提取的场景ID:")
for i, scene_id in enumerate(scene_ids, 1):
    print(f"  {i}. {scene_id}")
```

### 4. 与bbox集成使用

```python
# 导出为bbox兼容格式
with open("bbox_scene_ids.txt", "w") as f:
    for scene_id in scene_ids:
        f.write(f"{scene_id}\n")

print("场景ID已导出为bbox兼容格式: bbox_scene_ids.txt")
```

### 5. 处理失败的问题单

```python
# 检查处理统计
print(f"处理统计:")
print(f"  总计处理: {manager.stats['total_files']} 个URL")
print(f"  成功处理: {manager.stats['processed_files']} 个")
print(f"  失败处理: {manager.stats['failed_files']} 个")
print(f"  数据库查询失败: {manager.stats['defect_query_failed']} 个")
print(f"  无scene_id: {manager.stats['defect_no_scene']} 个")

# 如果失败率过高，进行故障排除
if manager.stats['failed_files'] > 0:
    print(f"\n失败率: {manager.stats['failed_files']/manager.stats['total_files']:.2%}")
    print("建议检查:")
    print("  1. 数据库连接是否正常")
    print("  2. URL格式是否正确")
    print("  3. 数据是否存在于数据库中")
```

### 6. 命令行使用

```bash
# 构建问题单数据集
python -m spdatalab.cli build-dataset \
    --index-file defect_urls.txt \
    --dataset-name "DefectAnalysisDataset" \
    --description "问题单分析数据集 v1.0" \
    --output defect_dataset.parquet \
    --format parquet \
    --defect-mode

# 查看数据集信息
python -m spdatalab.cli dataset-info \
    --dataset-file defect_dataset.parquet

# 导出场景ID
python -m spdatalab.cli list-scenes \
    --dataset-file defect_dataset.parquet \
    --output bbox_scene_ids.txt
```

这样就完成了从问题单URL到可用于bbox分析的scene_id的完整流程。 