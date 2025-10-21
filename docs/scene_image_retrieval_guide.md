# 场景图片检索工具使用指南

## 概述

场景图片检索工具提供了从OBS存储的parquet文件中快速获取和可视化autoscenes相机图片的能力。主要用于网格聚类分析后查看实际的场景图片。

**核心功能**：
- 从数据库查询场景OBS路径
- 读取parquet格式的相机图片数据
- 支持帧过滤（按索引或数量）
- 生成独立的HTML查看器（包含base64编码图片）
- 支持批量处理多个场景

**主要组件**：
- `SceneImageRetriever`: 图片检索器
- `SceneImageHTMLViewer`: HTML查看器生成器
- `view_cluster_images.py`: 集成示例脚本

## 快速入门

### 1. 基本用法

```python
from spdatalab.dataset.scene_image_retriever import SceneImageRetriever
from spdatalab.dataset.scene_image_viewer import SceneImageHTMLViewer

# 创建检索器
retriever = SceneImageRetriever()

# 加载单个场景的图片（前5帧）
frames = retriever.load_images_from_scene("scene_abc123", max_frames=5)

# 生成HTML报告
viewer = SceneImageHTMLViewer()
images_dict = {"scene_abc123": frames}
report_path = viewer.generate_html_report(
    images_dict, 
    "my_report.html",
    title="场景图片查看"
)

print(f"报告已生成: {report_path}")
```

### 2. 批量处理多个场景

```python
# 批量加载多个场景
scene_ids = ["scene_001", "scene_002", "scene_003"]
images = retriever.batch_load_images(scene_ids, frames_per_scene=3)

# 生成HTML报告
report_path = viewer.generate_html_report(
    images,
    "batch_report.html",
    title="批量场景查看"
)
```

### 3. 使用命令行工具

```bash
# 查看指定场景
python examples/dataset/bbox_examples/view_cluster_images.py \
    --scene-ids scene_001 scene_002 \
    --frames-per-scene 5 \
    --output my_report.html

# 从网格聚类结果查看
python examples/dataset/bbox_examples/view_cluster_images.py \
    --grid-id 123 \
    --cluster-label 0 \
    --max-scenes 10 \
    --frames-per-scene 3
```

## API参考

### SceneImageRetriever

#### 初始化

```python
retriever = SceneImageRetriever(camera_type="CAM_FRONT_WIDE_ANGLE")
```

**参数**：
- `camera_type` (str): 相机类型，默认 `"CAM_FRONT_WIDE_ANGLE"`

#### 方法

##### `get_scene_obs_paths(scene_ids: List[str]) -> pd.DataFrame`

批量查询场景的OBS路径。

**参数**：
- `scene_ids`: 场景ID列表

**返回**：
包含 `scene_id`, `data_name`, `scene_obs_path`, `timestamp` 的DataFrame

**示例**：
```python
df = retriever.get_scene_obs_paths(["scene_001", "scene_002"])
print(df)
```

##### `load_images_from_scene(scene_id, frame_indices=None, max_frames=None) -> List[ImageFrame]`

从单个场景加载图片。

**参数**：
- `scene_id` (str): 场景ID
- `frame_indices` (List[int], 可选): 指定要提取的帧索引，例如 `[0, 5, 10]`
- `max_frames` (int, 可选): 最大帧数限制

**返回**：
`ImageFrame` 对象列表

**示例**：
```python
# 加载前3帧
frames = retriever.load_images_from_scene("scene_123", max_frames=3)

# 加载指定帧
frames = retriever.load_images_from_scene("scene_123", frame_indices=[0, 10, 20])
```

##### `batch_load_images(scene_ids, frames_per_scene=5) -> Dict[str, List[ImageFrame]]`

批量加载多个场景的图片。

**参数**：
- `scene_ids` (List[str]): 场景ID列表
- `frames_per_scene` (int): 每个场景加载的帧数，默认5

**返回**：
字典，键为scene_id，值为ImageFrame列表

**示例**：
```python
scenes = ["scene_001", "scene_002", "scene_003"]
images = retriever.batch_load_images(scenes, frames_per_scene=3)
```

### ImageFrame

图片帧数据结构。

**属性**：
- `scene_id` (str): 场景ID
- `frame_index` (int): 帧索引
- `timestamp` (int): 时间戳（毫秒或微秒）
- `image_data` (bytes): 图片二进制数据
- `image_format` (str): 图片格式 ('png' 或 'jpeg')
- `filename` (str, 可选): 原始文件名

**方法**：
- `to_pil_image()`: 转换为PIL Image对象

**示例**：
```python
frame = frames[0]
print(f"场景: {frame.scene_id}")
print(f"帧索引: {frame.frame_index}")
print(f"格式: {frame.image_format}")

# 转换为PIL Image
pil_img = frame.to_pil_image()
pil_img.show()
```

### SceneImageHTMLViewer

#### 初始化

```python
viewer = SceneImageHTMLViewer()
```

#### 方法

##### `generate_html_report(images_dict, output_path, title="场景图片查看器", thumbnail_size=200) -> str`

生成HTML报告。

**参数**：
- `images_dict` (Dict[str, List[ImageFrame]]): 场景ID到ImageFrame列表的映射
- `output_path` (str): 输出HTML文件路径
- `title` (str): 报告标题，默认 `"场景图片查看器"`
- `thumbnail_size` (int): 缩略图最大尺寸（像素），默认200

**返回**：
生成的HTML文件路径（绝对路径）

**示例**：
```python
report_path = viewer.generate_html_report(
    images_dict,
    "report.html",
    title="聚类分析图片",
    thumbnail_size=250
)
```

## HTML查看器使用

### 特性

生成的HTML报告具有以下特性：

1. **独立文件**：所有图片以base64格式嵌入，无需外部依赖
2. **响应式布局**：自适应桌面和移动端
3. **场景分组**：按场景组织，支持折叠/展开
4. **缩略图预览**：200px缩略图，节省加载时间
5. **全屏查看**：点击图片查看全尺寸版本
6. **元数据显示**：显示帧索引、时间戳、格式等信息
7. **统计信息**：报告头部显示总场景数、总帧数等

### 交互操作

- **查看全屏图片**：点击任意缩略图
- **关闭全屏**：点击背景或按ESC键
- **折叠场景**：点击场景标题栏
- **浏览器缩放**：使用浏览器的缩放功能

### 浏览器兼容性

支持所有现代浏览器：
- Chrome/Edge 90+
- Firefox 88+
- Safari 14+

## 命令行工具详解

### view_cluster_images.py

从数据库查询场景并生成HTML报告的示例脚本。

#### 使用场景

##### 1. 查看指定场景

```bash
python examples/dataset/bbox_examples/view_cluster_images.py \
    --scene-ids scene_001 scene_002 scene_003 \
    --frames-per-scene 5 \
    --output my_scenes.html \
    --title "我的场景集"
```

##### 2. 从网格聚类结果查看

```bash
# 查看某个grid的特定cluster
python examples/dataset/bbox_examples/view_cluster_images.py \
    --grid-id 123 \
    --cluster-label 0 \
    --max-scenes 10 \
    --frames-per-scene 3

# 查看某个grid的所有cluster（不指定cluster-label）
python examples/dataset/bbox_examples/view_cluster_images.py \
    --grid-id 123 \
    --max-scenes 20 \
    --frames-per-scene 3
```

##### 3. 从分析ID查看TOP N clusters

```bash
python examples/dataset/bbox_examples/view_cluster_images.py \
    --analysis-id cluster_20231021 \
    --top-clusters 3 \
    --max-scenes-per-cluster 5 \
    --frames-per-scene 3
```

#### 完整参数说明

**场景来源参数（三选一）**：
- `--scene-ids`: 直接指定场景ID列表
- `--grid-id`: 从Grid聚类结果查询
- `--analysis-id`: 从分析ID查询

**Grid查询参数**：
- `--cluster-label`: 聚类标签（可选）
- `--max-scenes`: 最大场景数，默认10

**Analysis查询参数**：
- `--top-clusters`: 选择TOP N个cluster，默认3
- `--max-scenes-per-cluster`: 每个cluster的最大场景数，默认5

**图片加载参数**：
- `--frames-per-scene`: 每个场景加载的帧数，默认3
- `--camera-type`: 相机类型，默认 `CAM_FRONT_WIDE_ANGLE`

**输出参数**：
- `--output`: 输出HTML文件路径（可选，默认自动生成）
- `--title`: HTML报告标题（可选，默认自动生成）
- `--thumbnail-size`: 缩略图大小，默认200

## 集成示例

### 与网格聚类分析集成

```python
from sqlalchemy import create_engine, text
from spdatalab.dataset.scene_image_retriever import SceneImageRetriever
from spdatalab.dataset.scene_image_viewer import SceneImageHTMLViewer

# 1. 从聚类结果查询场景
engine = create_engine("postgresql+psycopg://postgres:postgres@local_pg:5432/postgres")

sql = text("""
    SELECT DISTINCT dataset_name
    FROM public.grid_trajectory_segments
    WHERE grid_id = :grid_id
      AND cluster_label = :cluster_label
      AND quality_flag = true
    LIMIT 10
""")

with engine.connect() as conn:
    result = conn.execute(sql, {"grid_id": 123, "cluster_label": 0})
    scene_ids = [row[0] for row in result]

print(f"找到 {len(scene_ids)} 个场景")

# 2. 加载图片
retriever = SceneImageRetriever()
images = retriever.batch_load_images(scene_ids, frames_per_scene=3)

# 3. 生成HTML报告
viewer = SceneImageHTMLViewer()
report_path = viewer.generate_html_report(
    images,
    "grid123_cluster0_images.html",
    title="Grid 123 - Cluster 0 图片样本"
)

print(f"报告已生成: {report_path}")
```

### 自定义帧选择策略

```python
# 加载每个场景的首帧、中间帧、末帧
def load_representative_frames(scene_id, retriever):
    # 先获取总帧数（加载全部）
    all_frames = retriever.load_images_from_scene(scene_id)
    
    if len(all_frames) <= 3:
        return all_frames
    
    # 选择首、中、末
    indices = [0, len(all_frames) // 2, len(all_frames) - 1]
    return retriever.load_images_from_scene(scene_id, frame_indices=indices)

# 批量处理
images_dict = {}
for scene_id in scene_ids:
    frames = load_representative_frames(scene_id, retriever)
    if frames:
        images_dict[scene_id] = frames
```

## 故障排除

### 常见问题

#### 1. 数据库查询返回空结果

**症状**：`scene_obs_path` 为空或None

**解决方案**：
- 检查scene_id是否正确
- 验证数据库表 `transform.ods_t_data_fragment_datalake` 是否包含该场景
- 确认该场景确实有 `scene_obs_path` 字段

```python
# 调试代码
df = retriever.get_scene_obs_paths(["your_scene_id"])
print(df)
```

#### 2. OBS文件访问失败

**症状**：`列出OBS目录失败` 或 `读取parquet文件失败`

**解决方案**：
- 检查环境变量配置（S3_ENDPOINT, ACCESS_KEY_ID等）
- 验证OBS访问权限
- 确认 `scene_obs_path` 路径格式正确

```python
# 检查环境变量
import os
print(os.getenv('S3_ENDPOINT'))
print(os.getenv('ACCESS_KEY_ID'))
```

#### 3. Parquet格式不匹配

**症状**：`未找到图片数据列` 或解析失败

**解决方案**：
- 检查parquet文件的schema
- 代码会自动适配常见列名（image, img_data, image_data等）
- 如果列名特殊，需要修改 `_parse_parquet_to_frames` 方法

```python
# 调试parquet结构
import pyarrow.parquet as pq
from spdatalab.common.file_utils import open_file

with open_file("obs://your/parquet/path", 'rb') as f:
    table = pq.read_table(f)
    print(table.schema)
    print(table.to_pandas().head())
```

#### 4. 图片解码失败

**症状**：`解析第 X 帧失败`

**解决方案**：
- 检查图片数据是否完整
- 验证图片格式（PNG/JPEG）
- 可能是数据损坏，跳过该帧继续处理

#### 5. HTML文件过大

**症状**：HTML文件超过100MB，浏览器加载慢

**解决方案**：
- 减少场景数量
- 减少每场景帧数（`frames_per_scene`参数）
- 增大 `thumbnail_size` 以减少缩略图质量（较小值=更小文件）
- 分批生成多个HTML文件

```python
# 分批处理
batch_size = 10
for i in range(0, len(scene_ids), batch_size):
    batch_ids = scene_ids[i:i+batch_size]
    images = retriever.batch_load_images(batch_ids, frames_per_scene=3)
    viewer.generate_html_report(
        images,
        f"report_batch_{i//batch_size}.html",
        title=f"批次 {i//batch_size}"
    )
```

### 调试模式

启用详细日志输出：

```python
import logging

# 设置日志级别为DEBUG
logging.basicConfig(level=logging.DEBUG)

# 或只设置特定模块
logger = logging.getLogger('spdatalab.dataset.scene_image_retriever')
logger.setLevel(logging.DEBUG)
```

## 性能优化建议

### 1. 批量加载最佳实践

- **批量大小**：建议每批10-50个场景
- **并行处理**：当前版本为串行，未来可增加多进程支持
- **内存管理**：大批量时注意内存使用

```python
# 大规模处理示例
def process_large_batch(scene_ids, batch_size=20):
    total = len(scene_ids)
    for i in range(0, total, batch_size):
        batch = scene_ids[i:i+batch_size]
        print(f"处理批次 {i//batch_size + 1}/{(total-1)//batch_size + 1}")
        
        images = retriever.batch_load_images(batch, frames_per_scene=3)
        report_path = f"report_batch_{i//batch_size}.html"
        viewer.generate_html_report(images, report_path)
```

### 2. 帧采样策略

根据需求选择合适的采样策略：

- **快速预览**：1-3帧（首帧即可）
- **代表性采样**：5-10帧（均匀分布）
- **详细分析**：所有帧（适合短场景）

```python
# 均匀采样
def uniform_sample(total_frames, target_count):
    if total_frames <= target_count:
        return list(range(total_frames))
    step = total_frames / target_count
    return [int(i * step) for i in range(target_count)]

# 使用
indices = uniform_sample(100, 10)  # 从100帧中均匀采样10帧
frames = retriever.load_images_from_scene(scene_id, frame_indices=indices)
```

### 3. HTML文件大小控制

- **目标大小**：建议 < 50MB（快速加载）
- **最大大小**：不超过100MB（浏览器限制）

估算公式：
```
文件大小(MB) ≈ 场景数 × 每场景帧数 × 平均图片大小(MB) × 1.37
```

其中1.37是base64编码的膨胀系数。

## 未来扩展

以下功能计划在未来版本中实现：

### 1. 多相机支持

```python
# 未来API（计划）
retriever = SceneImageRetriever(camera_type="ALL")  # 加载所有相机
images = retriever.load_images_from_scene(
    "scene_123",
    cameras=["CAM_FRONT", "CAM_BACK", "CAM_LEFT", "CAM_RIGHT"]
)
```

### 2. 本地缓存

```python
# 未来API（计划）
retriever = SceneImageRetriever(cache_dir="./image_cache")
# 自动缓存下载的图片，加速重复访问
```

### 3. 视频生成

```python
# 未来API（计划）
from spdatalab.dataset.scene_video_generator import SceneVideoGenerator

generator = SceneVideoGenerator()
generator.generate_video(
    frames,
    output_path="scene_video.mp4",
    fps=10
)
```

### 4. FiftyOne集成

```python
# 未来API（计划）
import fiftyone as fo

dataset = create_fiftyone_dataset(images_dict)
session = fo.launch_app(dataset)
```

## 测试指南

### 单元测试

```bash
# 运行所有单元测试
python -m pytest tests/test_scene_image_retriever.py -v

# 运行特定测试
python -m pytest tests/test_scene_image_retriever.py::TestSceneImageRetriever::test_detect_image_format_png -v
```

### 集成测试

需要真实环境（数据库+OBS）：

```bash
# 运行集成测试
python -m pytest tests/test_scene_image_retriever.py::TestIntegration -v -m integration

# 手动测试
python examples/dataset/bbox_examples/view_cluster_images.py \
    --scene-ids <有效的scene_id> \
    --max-frames 2 \
    --output test_report.html
```

### 测试检查清单

- [ ] 数据库连接正常
- [ ] 能够查询到scene_obs_path
- [ ] OBS文件访问权限正常
- [ ] Parquet文件读取成功
- [ ] 图片解码正常
- [ ] HTML文件生成成功
- [ ] 浏览器中图片显示正常

## 参考资料

- [Pandas Parquet文档](https://pandas.pydata.org/docs/reference/api/pandas.read_parquet.html)
- [PyArrow Parquet文档](https://arrow.apache.org/docs/python/parquet.html)
- [PIL/Pillow文档](https://pillow.readthedocs.io/)
- [网格聚类分析指南](./grid_trajectory_clustering.md)

## 联系与反馈

如有问题或建议，请联系开发团队或提交Issue。

