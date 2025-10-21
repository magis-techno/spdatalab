# 场景图片查看器 - 快速开始

## 简介

从autoscenes数据中快速提取和查看相机图片的工具，特别适用于网格聚类分析后的人工审查。

## 快速开始

### 1. 查看指定场景的图片

```bash
python view_cluster_images.py \
    --scene-ids scene_abc123 scene_def456 \
    --frames-per-scene 5 \
    --output my_scenes.html
```

### 2. 查看聚类结果中的图片

```bash
# 查看Grid 123, Cluster 0的场景
python view_cluster_images.py \
    --grid-id 123 \
    --cluster-label 0 \
    --max-scenes 10 \
    --frames-per-scene 3
```

### 3. 查看TOP 3个Cluster的图片

```bash
python view_cluster_images.py \
    --analysis-id cluster_20231021 \
    --top-clusters 3 \
    --max-scenes-per-cluster 5 \
    --frames-per-scene 3
```

## Python API使用

```python
from spdatalab.dataset.scene_image_retriever import SceneImageRetriever
from spdatalab.dataset.scene_image_viewer import SceneImageHTMLViewer

# 1. 检索图片
retriever = SceneImageRetriever()
images = retriever.batch_load_images(
    scene_ids=["scene_001", "scene_002"], 
    frames_per_scene=3
)

# 2. 生成HTML报告
viewer = SceneImageHTMLViewer()
report_path = viewer.generate_html_report(
    images, 
    "my_report.html",
    title="我的场景集"
)

print(f"报告已生成: {report_path}")
```

## 输出示例

HTML报告包含：
- 📊 统计信息（场景数、帧数）
- 🖼️ 场景分组展示
- 🔍 缩略图预览（点击查看全屏）
- 📝 元数据（帧索引、时间戳、格式）

## 常用参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--scene-ids` | 场景ID列表 | - |
| `--grid-id` | Grid ID | - |
| `--cluster-label` | 聚类标签 | 所有cluster |
| `--max-scenes` | 最大场景数 | 10 |
| `--frames-per-scene` | 每场景帧数 | 3 |
| `--output` | 输出路径 | 自动生成 |
| `--title` | 报告标题 | 自动生成 |

## 完整文档

详细使用说明请查看：
- [使用指南](../../../docs/scene_image_retrieval_guide.md)
- [测试说明](../../../docs/scene_image_retrieval_testing.md)

## 示例工作流

### 场景1：聚类分析后查看代表性图片

```bash
# 1. 运行聚类分析
python grid_clustering_analysis.py --grid-id 123

# 2. 查看Cluster 0的图片
python view_cluster_images.py --grid-id 123 --cluster-label 0 --max-scenes 5
```

### 场景2：对比不同Cluster的图片

```bash
# Cluster 0
python view_cluster_images.py \
    --grid-id 123 --cluster-label 0 \
    --output cluster0_images.html --title "Cluster 0"

# Cluster 1
python view_cluster_images.py \
    --grid-id 123 --cluster-label 1 \
    --output cluster1_images.html --title "Cluster 1"
```

### 场景3：批量查看多个Grid的图片

```bash
# 创建批处理脚本
for grid_id in 123 456 789; do
    python view_cluster_images.py \
        --grid-id $grid_id \
        --max-scenes 5 \
        --output "grid_${grid_id}_images.html"
done
```

## 性能建议

- **快速预览**：1-2帧/场景，5-10个场景
- **详细分析**：3-5帧/场景，10-20个场景
- **HTML大小控制**：建议 < 50MB（超过100MB会影响浏览器性能）

## 故障排除

### 问题：未找到场景的OBS路径

```bash
# 检查scene_id是否存在
psql -h local_pg -U postgres -d postgres -c \
  "SELECT id, scene_obs_path FROM transform.ods_t_data_fragment_datalake WHERE id = 'your_scene_id';"
```

### 问题：OBS访问失败

检查环境变量配置：
```bash
echo $S3_ENDPOINT
echo $ACCESS_KEY_ID
```

### 问题：HTML文件过大

减少场景数或每场景帧数：
```bash
python view_cluster_images.py \
    --scene-ids scene_001 scene_002 \
    --frames-per-scene 2  # 减少到2帧
```

## 相关工具

- `grid_clustering_analysis.py` - 网格轨迹聚类分析
- `analyze_grid_multimodal_similarity.py` - Grid多模态相似性分析
- `debug_export_grid_points.py` - Grid轨迹点导出

## 技术细节

- **相机类型**：默认 `CAM_FRONT_WIDE_ANGLE`（架构支持扩展）
- **图片格式**：PNG/JPEG（自动检测）
- **存储方式**：base64嵌入HTML（无外部依赖）
- **数据源**：OBS存储的parquet文件

## 测试

在远端环境运行测试：

```bash
# 单元测试
python -m pytest tests/test_scene_image_retriever.py -v

# 集成测试（需要有效scene_id）
python view_cluster_images.py --scene-ids <valid_scene_id> --output test.html
```

详细测试指南：[scene_image_retrieval_testing.md](../../../docs/scene_image_retrieval_testing.md)

