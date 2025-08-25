# 多模态轨迹检索系统 PRD

## 📋 产品概述

### 产品名称
多模态轨迹检索系统 (Multimodal Trajectory Retrieval System)

### 产品版本
v1.0 (基于spdatalab现有polygon_trajectory_query模块扩展)

### 产品定位
**研发分析专用**的轻量化轨迹检索工具，基于文本输入实现智能轨迹发现与邻近分析，专注于数据探索和算法验证场景。

## 🎯 核心价值主张

### 研发价值
1. **快速场景定位**：通过文本描述快速找到感兴趣的轨迹场景
2. **邻近模式发现**：基于发现的轨迹自动扩展分析区域，发现相关模式
3. **轻量化分析**：专注核心功能，快速验证和数据探索
4. **复用现有基础**：80%+复用现有高性能模块，开发成本低

### 技术价值
1. **技术栈复用**：最大化利用现有polygon_trajectory_query模块的80%功能
2. **架构扩展性**：模块化设计，易于后续功能扩展和维护
3. **性能继承**：自动获得现有系统的所有性能优化策略

## 🌟 产品功能

### 核心功能

#### 1. 多模态检索
**功能描述**：支持文本输入，通过多模态API检索相关的轨迹数据点

**输入方式**：
- **文本检索**：自然语言描述，如"bicycle crossing intersection"、"红色汽车转弯" 
- **图片检索**：*预留功能，暂不开发*

**检索参数**：
- **相机表选择**：ddi_collection_camera_encoded_{1,2,8,9,10,11,12} (自动匹配对应的camera_X)
- **时间范围**：支持start_time和end_time限制
- **结果数量**：支持count参数控制返回数量
- **相似度阈值**：过滤低相似度结果

**API限制**：
- **单次查询上限**：1万条数据
- **累计查询上限**：同一条件下多次查询总计最多10万条数据

**输出结果**：
```json
[
  {
    "dataset_name": "d934a91780f14e6fb3a5b01d5fbbb412_330034_2025/02/19/17:56:09-17:57:09",
    "timestamp": 1739958971349,
    "similarity": 0.8745,
    "metadata": {
      "img_path": "obs://ais-upload/xmodalitys_img/...",
      "dataset_bag": "camera_encoded_2"
    }
  }
]
```

#### 2. 轨迹数据获取
**功能描述**：基于检索到的dataset_name和timestamp，获取完整的轨迹数据

**核心能力**：
- 根据dataset_name批量查询完整轨迹
- 支持时间窗口扩展（默认30天）
- 自动scene_id和event_id映射
- 轨迹数据质量验证和清洗

**时间窗口策略**：
- 固定窗口：以检索timestamp为中心的时间范围（默认30天）

#### 3. 轨迹空间扩展
**功能描述**：将获取的轨迹线转换为polygon查询区域，用于发现邻近轨迹

**核心参数**：
- **时间窗口**：查询邻近轨迹的时间范围（默认30天）
- **空间缓冲区**：轨迹线周围固定距离缓冲（默认10米）

**几何优化**：
- 简化复杂polygon，优化查询性能
- 最小面积过滤，避免过小的无效区域

#### 4. 智能聚合优化
**功能描述**：对检索结果和polygon进行智能聚合，提升查询效率

**聚合策略**：
- **dataset_name聚合**：避免重复查询同一数据集
- **时间窗口聚合**：合并相近时间的查询，减少数据库访问
- **polygon合并**：自动合并重叠度高的polygon（默认阈值70%）
- **映射保持**：保留polygon到原始dataset_name和timestamp的完整映射

#### 5. 轻量化轨迹发现
**功能描述**：基于合并优化的polygon区域，发现相关轨迹数据

**查询优化**：
- 复用现有的HighPerformancePolygonTrajectoryQuery引擎
- 基于合并后的polygon减少查询复杂度
- 智能批量/分块查询策略自动选择

**轻量化输出**：
- **轨迹点优先**：返回轨迹点数据而非完整轨迹线
- **源信息保持**：每个轨迹点保留对应的源polygon信息
- **统计汇总**：提供聚合前后的对比统计

#### 6. 结果分析和导出
**功能描述**：对发现的轨迹点进行分析，支持多种格式导出

**分析维度**：
- **轨迹点统计**：总点数、时间分布、空间分布
- **聚合效果**：polygon合并比例、查询优化效果
- **源数据关联**：轨迹点到源dataset和polygon的映射关系

**导出格式**：
- **数据库表**：轨迹点表，包含源polygon映射信息
- **GeoJSON文件**：轨迹点的地理数据格式
- **聚合报告**：JSON格式的聚合优化统计

### 研发辅助功能

#### 1. 批量分析
- 支持批量文本查询
- 并行处理提升效率

#### 2. 质量控制
- 相似度阈值过滤
- 基础轨迹质量验证

#### 3. 分析统计
- 查询性能统计
- 结果数量统计
- 发现模式汇总

## 🏗️ 技术架构

### 优化的系统架构图
```
用户文本输入 
    ↓
多模态检索API调用
    ↓
智能聚合优化 (dataset_name + 时间窗口聚合)
    ↓
批量轨迹数据查询 (减少重复查询)
    ↓
轨迹→Polygon转换 + 重叠Polygon合并
    ↓
轻量化Polygon查询 (仅返回轨迹点)
    ↓
轨迹点 + 源映射信息导出
```

### 核心组件
1. **MultimodalRetriever**：多模态API调用和结果解析
2. **ResultAggregator**：智能聚合器（dataset_name + 时间窗口聚合）
3. **TrajectoryToPolygonConverter**：轨迹几何转换
4. **PolygonMerger**：Polygon合并优化器（重叠合并）
5. **MultimodalTrajectoryWorkflow**：轻量化工作流协调
6. **HighPerformancePolygonTrajectoryQuery**：复用现有查询引擎

### 优化的数据流
1. 用户文本输入 → 多模态API调用 → 检索候选结果
2. 检索结果 → 智能聚合（dataset_name + 时间窗口） → 优化查询参数
3. 聚合结果 → 批量轨迹查询 → 轨迹LineString数据
4. 轨迹数据 → Polygon转换 → 原始Polygon列表
5. 原始Polygon → 重叠合并优化 → 合并后Polygon + 源映射
6. 合并Polygon → 轻量化查询 → 轨迹点数据
7. 轨迹点 → 源信息关联 → 最终结果导出

## 🎨 用户界面设计

### 命令行界面(CLI)

#### 基础用法
```bash
# 文本查询（研发分析）
python -m spdatalab.dataset.multimodal_trajectory_retrieval \
    --text "bicycle crossing intersection" \
    --collection "ddi_collection_camera_encoded_1" \
    --output-table "discovered_trajectories" \
    --buffer-distance 10

# 注：camera参数自动从collection中推导 (camera_1 对应 ddi_collection_camera_encoded_1)
```

#### 高级参数
```bash
# 完整参数示例（研发分析）
python -m spdatalab.dataset.multimodal_trajectory_retrieval \
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
    --verbose

# API限制：单次最多10000条，累计最多100000条
```

### Python API

#### 基础API
```python
from spdatalab.dataset.multimodal_trajectory_retrieval import (
    MultimodalTrajectoryWorkflow,
    MultimodalConfig
)

# 配置
config = MultimodalConfig(
    api_config={
        "project": "your_project",
        "api_key": "your_api_key",
        "username": "your_username"
    },
    buffer_distance=100.0,
    similarity_threshold=0.7
)

# 工作流执行
workflow = MultimodalTrajectoryWorkflow(config)

# 文本查询（研发分析）
result = workflow.process_text_query(
    text="bicycle crossing intersection",
    collection="ddi_collection_camera_encoded_1",  # camera_1自动推导
    count=5000,  # API限制：单次最多10000
    output_table="discovered_trajectories"
)

# 图片查询预留接口（暂不实现）
# result = workflow.process_image_query(...)
```

#### 高级API
```python
# 自定义配置
from spdatalab.dataset.polygon_trajectory_query import PolygonTrajectoryConfig

config = MultimodalConfig(
    api_config={...},
    max_search_results=50,
    time_window_days=30,    # 时间窗口（天）
    buffer_distance=10.0,   # 空间缓冲区（米）
    polygon_config=PolygonTrajectoryConfig(
        batch_threshold=50,
        chunk_size=20,
        limit_per_polygon=15000
    )
)
```

## 📊 性能指标

### 性能目标

#### 响应时间
- **小规模查询**（1-5个候选轨迹）：< 30秒
- **中规模查询**（5-20个候选轨迹）：< 2分钟
- **大规模查询**（20-100个候选轨迹）：< 10分钟

#### 吞吐量
- **并发查询**：支持3-5个并发查询
- **批量处理**：支持单次处理100+个查询请求
- **数据处理**：10万+轨迹点/分钟的处理能力

#### 准确性
- **检索精度**：相似度阈值0.3以上的结果准确率>85%
- **轨迹完整性**：获取轨迹的完整度>90%
- **空间关联性**：发现的邻近轨迹相关性>80%

### 资源需求

#### 计算资源
- **CPU**：多核处理器，支持并行几何计算
- **内存**：8GB+，支持大规模轨迹数据处理
- **存储**：SSD推荐，优化数据库查询性能

#### 网络资源
- **API调用**：稳定的外网连接，支持多模态API访问
- **数据库连接**：高速内网连接，支持大数据量传输

## 🔒 基础要求

### 研发环境配置
1. **API密钥**：环境变量配置
2. **网络访问**：需要访问多模态API
3. **数据库连接**：PostgreSQL + 现有Hive连接

## 🚀 开发计划

### Phase 1: 核心功能 (1周)
- 多模态API集成（仅文本）
- 轨迹查询和转换
- 基础工作流

**交付物**：
- 可用的原型
- 基础测试

### Phase 2: 完善优化 (1周)  
- 批量处理
- 错误处理
- 性能优化

**交付物**：
- 完整功能版本
- 使用文档

## 📈 验收标准

### 功能完整性
- **文本检索**：可用的多模态API集成
- **轨迹发现**：基于文本查询发现相关轨迹
- **空间分析**：基于缓冲区的邻近轨迹查询
- **结果导出**：支持数据库表和GeoJSON输出

### 技术指标
- **API限制遵守**：单次≤1万条，累计≤10万条
- **代码复用率**：≥80%复用现有模块
- **响应时间**：研发分析场景下的合理响应时间
- **易用性**：简单的CLI和API接口

## 🎯 主要风险

### 技术风险
1. **API依赖**：多模态API的稳定性和可用性
   - **缓解**：基础重试机制，错误提示
2. **数据量限制**：API限制可能影响分析深度
   - **缓解**：分批查询，合理使用限额
3. **集成复杂性**：与现有系统的集成
   - **缓解**：最大化复用现有模块，最小化新增代码

### 使用风险  
1. **学习成本**：新工具的使用门槛
   - **缓解**：简化参数，提供使用示例
2. **结果准确性**：多模态检索的准确性
   - **缓解**：支持相似度阈值调整，结果验证

## 📚 附录

### 相关文档
- [现有polygon_trajectory_query使用指南](./polygon_trajectory_query_guide.md)
- [多模态API文档](https://your-api-server.com/xmodalitys)
- [spdatalab开发规范](../README.md)

### 参考资源
- Ring平台接口规范
- PostgreSQL空间数据处理最佳实践
- 高性能轨迹分析算法参考

### 版本历史
- v1.0 (2024): 初始版本PRD
  - 核心功能定义
  - 技术架构设计
  - 发布计划制定
