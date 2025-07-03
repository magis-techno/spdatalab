# URL格式数据集处理指南

本指南介绍如何使用spdatalab处理URL格式的数据集，将问题单URL转换为scene_id并生成边界框数据。

## 概述

新增的URL处理功能允许您：
1. 从问题单URL中提取dataName
2. 通过数据库查询获取对应的scene_id
3. 生成标准的数据集格式供bbox处理使用

## 处理流程

```
URL → dataName → defect_id → scene_id → 边界框数据
```

具体步骤：
1. 从URL中解析出`dataName`参数
2. 在`elasticsearch_ros.ods_ddi_index002_datalake`表中查询对应的`defect_id`
3. 在`transform.ods_t_data_fragment_datalake`表中查询对应的`scene_id`
4. 使用获得的scene_id生成边界框数据

## 支持的URL格式

URL示例：
```
https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=10000_ddi-application-667754027299119535
```

从中提取的dataName为：`10000_ddi-application-667754027299119535`

## 文件格式

### 1. URL文件格式

每行包含一个问题单URL：

```
https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=10000_ddi-application-667754027299119535
https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=10000_ddi-application-667754027299119536
https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=10000_ddi-application-667754027299119537
```

### 2. 索引文件格式

支持多种格式：

**URL格式（推荐）：** 直接放URL，无需@duplicate后缀
```
https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=10000_ddi-application-667754027299119535
https://pre-prod.adscloud.huawei.com/ddi-app/#/layout/ddi-system-evaluation/event-list-detail?dataName=10000_ddi-application-667754027299119536
```

**传统格式：** 路径+重复因子
```
/path/to/urls.txt@duplicate1
/path/to/more_urls.txt@duplicate2
```

## 使用方法

### 1. 直接处理URL文件

```python
from spdatalab.dataset.dataset_manager import DatasetManager

# 创建数据集管理器（自动支持URL和传统格式）
dataset_manager = DatasetManager()

# 从URL文件提取scene_id
scene_ids = dataset_manager.extract_scene_ids_from_urls("urls.txt")
print(f"提取到 {len(scene_ids)} 个scene_id")
```

### 2. 创建数据集并处理边界框

```python
from spdatalab.dataset.dataset_manager import DatasetManager
from spdatalab.dataset.bbox import run_with_partitioning

# 1. 创建数据集
dataset_manager = DatasetManager()
dataset = dataset_manager.build_dataset_from_index(
    "index.txt",  # 包含URL路径的索引文件
    "URL数据集",
    "从问题单URL生成的数据集"
)

# 2. 保存数据集
dataset_manager.save_dataset(dataset, "url_dataset.json")

# 3. 处理边界框
run_with_partitioning(
    "url_dataset.json",
    batch=100,
    use_parallel=True,
    create_unified_view_flag=True
)
```

### 3. 命令行使用

```bash
# 使用bbox命令处理URL格式的数据集
python -m spdatalab.dataset.bbox --input url_dataset.json --batch 100

# 使用分表模式并行处理
python -c "
from spdatalab.dataset.bbox import run_with_partitioning
run_with_partitioning('url_dataset.json', use_parallel=True, max_workers=4)
"
```

## 自动格式检测

系统会自动检测文件格式：

- **URL格式**：包含`http://`或`https://`且含有`dataName=`参数
- **JSONL路径格式**：包含`obs://`或`/god/`且含有`@duplicate`
- **其他格式**：回退到传统文本格式处理

```python
# 自动格式检测示例
dataset_manager = DatasetManager()
file_format = dataset_manager.detect_file_format("input.txt")
print(f"检测到格式: {file_format}")  # 'url', 'jsonl_path', 或 'unknown'
```

## 数据库配置

URL处理功能需要访问以下数据库表：

1. `elasticsearch_ros.ods_ddi_index002_datalake` - 用于查询defect_id
2. `transform.ods_t_data_fragment_datalake` - 用于查询scene_id

确保您的环境中`hive_cursor()`可以正常连接到这些数据库。

## 错误处理

系统提供了robust的错误处理：

- **URL解析失败**：记录警告并跳过该行
- **数据库查询失败**：记录错误详情并继续处理其他URL
- **格式检测失败**：自动回退到传统格式处理

### 查看处理统计

```python
dataset_manager = DatasetManager()
# 处理后检查统计信息
print(f"成功处理: {dataset_manager.stats['processed_files']} 个文件")
print(f"失败: {dataset_manager.stats['failed_files']} 个文件")
print(f"总scene数: {dataset_manager.stats['total_scenes']}")
```

## 性能优化

### 1. 批量处理

URL处理会自动进行批量数据库查询以提高性能。

### 2. 并行处理

在边界框生成阶段支持并行处理：

```python
run_with_partitioning(
    "url_dataset.json",
    use_parallel=True,
    max_workers=8,  # 调整worker数量
    batch=200       # 调整批次大小
)
```

### 3. 分表存储

URL生成的数据集支持分表存储，提高查询性能：

- 每个URL数据源创建独立的分表
- 自动生成统一视图方便查询
- 支持QGIS兼容的物化视图

## 示例和测试

查看 `examples/url_dataset_example.py` 获取完整的使用示例。

运行测试：
```bash
python examples/url_dataset_example.py
```

## 向后兼容性

新功能完全向后兼容：

- 现有的JSONL路径格式继续支持
- 原有的API保持不变
- 自动格式检测，无需额外配置

## 故障排除

### 常见问题

1. **URL解析失败**
   - 检查URL格式是否正确
   - 确认URL中包含`dataName`参数

2. **数据库连接失败**
   - 检查`hive_cursor()`配置
   - 确认有权限访问目标数据库表

3. **scene_id查询为空**
   - 检查dataName在数据库中是否存在
   - 确认defect_id与scene_id的关联关系

### 调试模式

启用详细日志：
```python
import logging
logging.getLogger('spdatalab.dataset.dataset_manager').setLevel(logging.DEBUG)
```

## 总结

URL格式处理功能让您能够：
- 直接从问题单URL生成边界框数据
- 保持与现有工作流的兼容性
- 享受自动格式检测和错误处理
- 利用并行处理提高性能

更多信息请参考源码中的详细注释和测试用例。 