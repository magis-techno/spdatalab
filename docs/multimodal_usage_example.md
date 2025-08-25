# 多模态轨迹检索系统 - 使用示例

## 📋 Day 1 开发成果

### 已完成功能
✅ **基础数据检索模块** (`dataset/multimodal_data_retriever.py`)
- `MultimodalRetriever` - 多模态API调用器
- `TrajectoryToPolygonConverter` - 轨迹转Polygon转换器
- API限制控制（单次1万条，累计10万条）
- 相机自动匹配（collection → camera参数推导）

✅ **主融合分析模块** (`fusion/multimodal_trajectory_retrieval.py`)
- `MultimodalTrajectoryWorkflow` - 主工作流协调器
- `ResultAggregator` - 智能聚合器（dataset_name + 时间窗口聚合）
- `PolygonMerger` - Polygon合并优化器（重叠合并）
- 轻量化工作流设计（返回轨迹点，非完整轨迹）

✅ **CLI接口** (`fusion/multimodal_cli.py`)
- 完整的命令行参数支持
- 环境变量配置管理
- 详细的帮助文档和错误提示

## 🚀 使用方法

### 1. 环境配置

#### 方式1: 使用.env文件（推荐）

在项目根目录创建 `.env` 文件：

```bash
# 必需配置
MULTIMODAL_PROJECT=your_project
MULTIMODAL_API_KEY=your_api_key
MULTIMODAL_USERNAME=your_username

# 可选配置
MULTIMODAL_API_URL=https://driveinsight-api.ias.huawei.com/xmodalitys
MULTIMODAL_TIMEOUT=30
MULTIMODAL_MAX_RETRIES=3
```

详细配置说明请参考：[环境变量配置示例](./env_config_example.md)

#### 方式2: 直接设置环境变量

```bash
# 必需变量
export MULTIMODAL_PROJECT="your_project"
export MULTIMODAL_API_KEY="your_api_key"
export MULTIMODAL_USERNAME="your_username"

# 可选变量
export MULTIMODAL_API_URL="https://driveinsight-api.ias.huawei.com/xmodalitys"
export MULTIMODAL_TIMEOUT="30"
export MULTIMODAL_MAX_RETRIES="3"
```

### 2. 基础命令行使用

```bash
# 基础文本查询
python -m spdatalab.fusion.multimodal_trajectory_retrieval \
    --text "bicycle crossing intersection" \
    --collection "ddi_collection_camera_encoded_1" \
    --output-table "discovered_trajectories"

# 完整参数示例
python -m spdatalab.fusion.multimodal_trajectory_retrieval \
    --text "red car turning left" \
    --collection "ddi_collection_camera_encoded_1" \
    --count 5000 \
    --similarity-threshold 0.7 \
    --start-time 1739958000000 \
    --end-time 1739959000000 \
    --time-window 30 \
    --buffer-distance 10 \
    --output-table "red_car_trajectories" \
    --output-geojson "red_car_results.geojson" \
    --output-json "complete_results.json" \
    --verbose
```

### 3. Python API使用

#### 方式1: 使用环境变量配置（推荐）

```python
from spdatalab.dataset.multimodal_data_retriever import APIConfig
from spdatalab.fusion.multimodal_trajectory_retrieval import (
    MultimodalConfig,
    MultimodalTrajectoryWorkflow
)

# 从环境变量自动创建API配置
api_config = APIConfig.from_env()

# 创建多模态配置
config = MultimodalConfig(
    api_config=api_config,
    buffer_distance=10.0,
    similarity_threshold=0.7
)

# 工作流执行
workflow = MultimodalTrajectoryWorkflow(config)

# 文本查询
result = workflow.process_text_query(
    text="bicycle crossing intersection",
    collection="ddi_collection_camera_encoded_1",
    count=5000
)

print(f"发现轨迹点: {result['summary']['total_points']}")
print(f"优化效果: {result['summary']['optimization_ratio']}")
```

#### 方式2: 手动创建配置

```python
from spdatalab.dataset.multimodal_data_retriever import APIConfig
from spdatalab.fusion.multimodal_trajectory_retrieval import (
    MultimodalConfig,
    MultimodalTrajectoryWorkflow
)

# 手动创建API配置
api_config = APIConfig(
    project="your_project",
    api_key="your_api_key",
    username="your_username",
    api_url="https://driveinsight-api.ias.huawei.com/xmodalitys",  # 可自定义
    timeout=30,
    max_retries=3
)

# 其余代码相同...
config = MultimodalConfig(api_config=api_config, ...)
```

## 🔧 核心特性

### 智能聚合策略
- **Dataset聚合**：避免重复查询相同数据集
- **时间窗口聚合**：合并相近时间的查询，减少数据库访问
- **Polygon合并**：自动合并重叠度高的polygon（默认阈值70%）

### API限制管理
- **单次查询限制**：最多10,000条（硬限制）
- **累计查询限制**：最多100,000条（会话级限制）
- **自动计数**：实时追踪查询使用量

### 相机自动匹配
```python
# 自动推导逻辑
"ddi_collection_camera_encoded_1"  → "camera_1"
"ddi_collection_camera_encoded_12" → "camera_12"
```

### 轻量化输出
- **轨迹点优先**：返回轨迹点数据而非完整轨迹线
- **源信息保持**：每个轨迹点保留对应的源polygon信息
- **优化统计**：显示聚合前后的对比效果

## 📊 输出格式

### 查询结果结构
```json
{
  "trajectory_points": [
    {
      "dataset_name": "dataset_1",
      "timestamp": 1739958971349,
      "longitude": 116.3,
      "latitude": 39.9,
      "source_polygon_id": "merged_polygon_0"
    }
  ],
  "source_polygons": [
    {
      "id": "merged_polygon_0",
      "properties": {
        "merged_count": 2,
        "sources": [...],
        "merge_type": "overlapping"
      },
      "geometry_wkt": "POLYGON(...)"
    }
  ],
  "summary": {
    "total_points": 1250,
    "unique_datasets": 5,
    "polygon_sources": 3,
    "optimization_ratio": "8 → 3"
  },
  "stats": {
    "search_results_count": 15,
    "aggregated_datasets": 5,
    "raw_polygon_count": 8,
    "merged_polygon_count": 3,
    "total_duration": 45.2
  }
}
```

## ⚡ 性能优化特性

1. **智能聚合**：减少重复查询，优化数据库访问
2. **Polygon合并**：合并重叠区域，减少空间查询复杂度
3. **并行处理**：轨迹转换支持多线程并行
4. **轻量化查询**：仅返回必要的轨迹点数据

## 🔍 调试和监控

### 详细日志
```bash
# 启用详细输出
python -m spdatalab.fusion.multimodal_trajectory_retrieval \
    --text "your query" \
    --collection "your_collection" \
    --verbose
```

### 查询统计
```python
# 获取API使用统计
retriever = MultimodalRetriever(api_config)
stats = retriever.get_query_stats()
print(f"已使用: {stats['total_queries']}")
print(f"剩余: {stats['remaining_queries']}")
```

## 🚧 待开发功能 (Day 2-3)

- [ ] 集成现有的`HighPerformancePolygonTrajectoryQuery`模块
- [ ] 实现真实的轨迹数据获取（替换模拟数据）
- [ ] 完善数据库写入和GeoJSON导出功能
- [ ] 添加错误处理和重试机制
- [ ] 性能优化和批量处理

## 📝 技术特点

- **80%+代码复用**：基于现有`polygon_trajectory_query`模块
- **轻量化设计**：专注研发分析需求
- **模块化架构**：清晰的职责分离
- **配置驱动**：灵活的参数调整
- **研发友好**：简化的API和命令行接口

---

**开发状态**: Day 1 完成 ✅  
**下一步**: Day 2 聚合优化和轨迹处理集成
