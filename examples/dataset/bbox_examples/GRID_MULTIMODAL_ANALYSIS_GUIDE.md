# Grid多模态相似性分析使用指南

## 概述

`analyze_grid_multimodal_similarity.py` 脚本用于分析冗余grid中的多模态数据相似性。通过结合空间冗余分析和多模态检索，可以深入了解特定区域内数据的相似性分布。

## 工作流程

```
1. 冗余分析 (analyze_spatial_redundancy.py)
   ↓ 生成 city_grid_density 表
   
2. 选择目标Grid
   ↓ 根据城市ID和排名选择高冗余grid
   
3. 提取Grid数据
   ↓ 通过空间连接获取grid内的dataset_name列表
   
4. 多模态检索
   ↓ 使用文本查询，按城市和dataset过滤
   
5. 相似度分析
   ↓ 统计分布、直方图、top结果
```

## 前置条件

### 1. 数据准备

确保已经运行过以下步骤：

```bash
# 创建基础表
python analyze_spatial_redundancy.py --create-table

# 生成grid密度数据
python batch_grid_analysis.py

# 验证数据（可选）
python analyze_spatial_redundancy.py --cities A72
```

### 2. 多模态API配置

在 `.env` 文件中配置多模态API：

```bash
# 多模态API配置
MULTIMODAL_PROJECT=your_project
MULTIMODAL_API_KEY=your_api_key
MULTIMODAL_USERNAME=your_username
MULTIMODAL_API_BASE_URL=https://driveinsight-api.ias.huawei.com
MULTIMODAL_API_PATH=/xmodalitys/retrieve
MULTIMODAL_PLATFORM=xmodalitys-external
MULTIMODAL_REGION=RaD-prod
MULTIMODAL_ENTRYPOINT_VERSION=v2
```

## 使用示例

### 基础使用

```bash
# 分析A72城市的最高冗余grid
python analyze_grid_multimodal_similarity.py --city A72
```

### 指定参数

```bash
# 分析第2高冗余的grid，查询"夜晚"相关图片
python analyze_grid_multimodal_similarity.py \
    --city A72 \
    --grid-rank 2 \
    --query-text "夜晚" \
    --max-results 200
```

### 指定相机和日期

```bash
# 使用camera_2，指定分析日期
python analyze_grid_multimodal_similarity.py \
    --city A263 \
    --collection ddi_collection_camera_encoded_2 \
    --analysis-date 2025-10-09
```

### 显示更多结果

```bash
# 显示top 20个最相似结果
python analyze_grid_multimodal_similarity.py \
    --city A72 \
    --top-n 20 \
    --max-results 500
```

## 命令行参数

| 参数 | 说明 | 默认值 | 必需 |
|------|------|--------|------|
| `--city` | 城市ID（如A72, A263） | - | 是 |
| `--grid-rank` | Grid排名（1=最高冗余） | 1 | 否 |
| `--query-text` | 查询文本 | "白天" | 否 |
| `--collection` | Collection名称 | ddi_collection_camera_encoded_1 | 否 |
| `--max-results` | 最大返回结果数 | 100 | 否 |
| `--analysis-date` | 分析日期（YYYY-MM-DD） | 最新日期 | 否 |
| `--top-n` | 显示top N个结果 | 10 | 否 |

## 输出说明

### 1. Grid信息
```
📍 选择Grid: A72 城市, Rank #1
   分析日期: 2025-10-09
   Grid坐标: (123, 456)
   BBox数量: 245
   Scene数量: 89
   Dataset数量: 12
```

### 2. Dataset列表
```
📦 Grid内的数据集 (前10个):
   1. dataset_a_2025/05/29/16:31:23 (23 scenes)
   2. dataset_b_2025/05/29/17:00:00 (15 scenes)
   ...
```

### 3. API调用结果
```
🔍 调用多模态API
   查询文本: '白天'
   Collection: ddi_collection_camera_encoded_1
   城市过滤: A72
   Dataset过滤: 12 个
   最大结果数: 100
✅ API调用成功: 返回 87 条结果
```

### 4. 相似度分析
```
📊 相似度分析
📈 相似度统计:
   范围: 0.234 ~ 0.891
   平均: 0.567
   中位数: 0.582
   样本数: 87

📊 相似度分布直方图:
   0.2-0.3: ████ (8, 9.2%)
   0.3-0.4: ████████ (15, 17.2%)
   0.4-0.5: ████████████ (23, 26.4%)
   0.5-0.6: ████████████████ (31, 35.6%)
   0.6-0.7: ████████ (10, 11.5%)
```

### 5. Top结果
```
🔝 Top 10 最相似结果:
   1. 相似度: 0.891
      Dataset: dataset_a_2025/05/29/16:31:23
      Timestamp: 1748507506699
      图片路径: obs://yw-xmodalitys-gy1/xmodalitys_img/xxx/camera_1/xxx.jpg
```

## 应用场景

### 1. 数据质量评估
通过分析同一区域内数据的相似度分布，评估数据采集的多样性：
- 相似度集中在高值 → 数据冗余度高
- 相似度分布均匀 → 数据多样性好

### 2. 场景选择优化
为训练集选择高质量、低冗余的场景：
- 选择相似度较低的图片
- 避免选择过于相似的重复场景

### 3. 城市特征分析
不同查询文本可以揭示城市的特征：
- "白天" vs "夜晚"
- "晴天" vs "雨天"
- "拥堵" vs "畅通"

### 4. 相机对比
通过不同collection对比不同相机的数据特征。

## 故障排查

### 问题1: 未找到Grid数据
```
❌ 未找到城市 A72 的Grid数据
```

**解决方案：**
```bash
# 1. 确认城市ID是否正确
# 2. 确认已运行过 batch_grid_analysis.py
python batch_grid_analysis.py

# 3. 查看可用城市
python analyze_spatial_redundancy.py
```

### 问题2: API调用失败
```
❌ API调用失败: 400 Client Error
```

**解决方案：**
1. 检查 `.env` 配置是否正确
2. 确认API_KEY是否有效
3. 检查网络连接
4. 查看详细错误日志

### 问题3: 返回结果为空
```
⚠️ API未返回结果
```

**可能原因：**
1. 查询文本与grid内的数据不匹配 → 尝试更通用的查询词
2. Dataset过滤过于严格 → 检查grid内是否有足够的dataset
3. 城市过滤不正确 → 确认城市ID格式

### 问题4: Dataset数量过多
```
⚠️ Dataset数量较多，API调用可能需要较长时间
```

**说明：**
- 这是正常情况，API需要在多个dataset中检索
- 可以通过 `--max-results` 控制返回数量
- 或选择不同的grid（通过 `--grid-rank`）

## 进阶用法

### 批量分析多个城市

创建脚本 `batch_analyze_cities.sh`：

```bash
#!/bin/bash
cities=("A72" "A263" "B001")

for city in "${cities[@]}"; do
    echo "分析城市: $city"
    python analyze_grid_multimodal_similarity.py \
        --city "$city" \
        --max-results 200 \
        > "results_${city}.txt"
done
```

### 对比不同查询文本

```bash
queries=("白天" "夜晚" "雨天" "晴天")

for query in "${queries[@]}"; do
    python analyze_grid_multimodal_similarity.py \
        --city A72 \
        --query-text "$query" \
        > "similarity_${query}.txt"
done
```

### 分析多个Grid排名

```bash
for rank in {1..5}; do
    python analyze_grid_multimodal_similarity.py \
        --city A72 \
        --grid-rank $rank \
        > "grid_rank_${rank}.txt"
done
```

## 数据导出

脚本输出可以重定向到文件进行进一步分析：

```bash
# 导出分析结果
python analyze_grid_multimodal_similarity.py --city A72 > analysis_a72.txt

# 提取相似度数据（使用grep）
grep "相似度:" analysis_a72.txt > similarities.txt

# 提取图片路径
grep "图片路径:" analysis_a72.txt > image_paths.txt
```

## 后续开发计划

### 1. 图片下载功能
```python
# 待实现
def download_images(results, output_dir):
    """下载检索结果中的图片"""
    pass
```

### 2. 视觉相似度分析
```python
# 待实现
def visual_similarity_analysis(image_paths):
    """使用图像处理库分析视觉相似度"""
    pass
```

### 3. 结果持久化
将分析结果保存到数据库表，便于后续查询和可视化。

### 4. QGIS可视化
在地图上标注检索结果的空间分布。

## 参考资料

- [多模态API文档](multimodal_data_retriever.py)
- [空间冗余分析指南](analyze_spatial_redundancy.py)
- [Grid聚类分析](grid_clustering_analysis.py)

## 联系与反馈

如有问题或建议，请参考项目README或联系开发团队。

