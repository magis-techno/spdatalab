# 场景图片查看器 - 快速测试命令

## 测试前准备

1. 确保在远端测试环境（有数据库和OBS访问权限）
2. 准备一个有效的scene_id（可以从数据库查询）

```sql
-- 查询有效的scene_id
SELECT id AS scene_id, origin_name, scene_obs_path 
FROM transform.ods_t_data_fragment_datalake 
WHERE scene_obs_path IS NOT NULL 
LIMIT 5;
```

## 快速测试（5分钟）

### 测试1：基础功能测试

```bash
# 替换为实际的scene_id
python examples/dataset/bbox_examples/view_cluster_images.py \
    --scene-ids <your_scene_id> \
    --frames-per-scene 2 \
    --output test_basic.html
```

**预期结果**：
- 生成 `test_basic.html` 文件
- 在浏览器中打开能看到2帧图片
- 图片可以点击放大

### 测试2：批量场景测试

```bash
# 使用多个scene_id
python examples/dataset/bbox_examples/view_cluster_images.py \
    --scene-ids <scene_id_1> <scene_id_2> <scene_id_3> \
    --frames-per-scene 3 \
    --output test_batch.html
```

**预期结果**：
- 生成包含多个场景的HTML
- 每个场景有独立的折叠区域

### 测试3：从Grid聚类查询（如果有聚类数据）

```bash
# 首先查询有效的grid_id
psql -h local_pg -U postgres -d postgres -c \
  "SELECT DISTINCT grid_id FROM public.grid_trajectory_segments LIMIT 5;"

# 使用查询到的grid_id
python examples/dataset/bbox_examples/view_cluster_images.py \
    --grid-id <grid_id> \
    --cluster-label 0 \
    --max-scenes 3 \
    --frames-per-scene 2 \
    --output test_cluster.html
```

**预期结果**：
- 从聚类结果加载场景
- 生成HTML报告

## Python API测试

```python
# 创建文件 test_api.py
from spdatalab.dataset.scene_image_retriever import SceneImageRetriever
from spdatalab.dataset.scene_image_viewer import SceneImageHTMLViewer

# 替换为实际的scene_id
scene_id = "your_actual_scene_id"

print("测试1：初始化")
retriever = SceneImageRetriever()
print("✅ SceneImageRetriever初始化成功")

print("\n测试2：查询OBS路径")
df = retriever.get_scene_obs_paths([scene_id])
print(f"✅ 查询到 {len(df)} 个场景的OBS路径")
print(df)

print("\n测试3：加载图片")
frames = retriever.load_images_from_scene(scene_id, max_frames=2)
print(f"✅ 加载了 {len(frames)} 帧图片")

if frames:
    frame = frames[0]
    print(f"   场景: {frame.scene_id}")
    print(f"   帧索引: {frame.frame_index}")
    print(f"   格式: {frame.image_format}")
    print(f"   大小: {len(frame.image_data)/1024:.1f} KB")

print("\n测试4：生成HTML")
viewer = SceneImageHTMLViewer()
images_dict = {scene_id: frames}
report_path = viewer.generate_html_report(
    images_dict,
    "test_api.html",
    title="API测试"
)
print(f"✅ HTML报告已生成: {report_path}")
```

运行测试：
```bash
python test_api.py
```

## 单元测试

```bash
# 运行所有单元测试
python -m pytest tests/test_scene_image_retriever.py -v

# 只运行快速测试
python -m pytest tests/test_scene_image_retriever.py::TestImageFrame -v
python -m pytest tests/test_scene_image_retriever.py::TestSceneImageRetriever::test_detect_image_format_png -v
```

## 测试检查清单

完成测试后，确认以下内容：

- [ ] 数据库查询返回有效的scene_obs_path
- [ ] OBS文件访问正常（没有权限错误）
- [ ] Parquet文件读取成功
- [ ] 图片解码正常（PNG/JPEG）
- [ ] HTML文件生成成功
- [ ] HTML在浏览器中正常打开
- [ ] 图片显示正常（缩略图和全屏）
- [ ] 元数据显示正确
- [ ] 场景折叠/展开功能正常

## 如果测试失败

### 数据库连接问题

```bash
# 测试数据库连接
python -c "
from spdatalab.common.io_hive import hive_cursor
with hive_cursor() as cur:
    cur.execute('SELECT 1')
    print('✅ 数据库连接正常')
"
```

### OBS访问问题

```bash
# 检查环境变量
python -c "
import os
print('S3_ENDPOINT:', os.getenv('S3_ENDPOINT'))
print('ACCESS_KEY_ID:', os.getenv('ACCESS_KEY_ID'))
print('Has SECRET:', bool(os.getenv('SECRET_ACCESS_KEY')))
"
```

### Parquet读取问题

```python
# 检查parquet文件结构
import pyarrow.parquet as pq
from spdatalab.common.file_utils import open_file

# 使用实际的OBS路径
obs_path = "obs://your/actual/parquet/path"

try:
    with open_file(obs_path, 'rb') as f:
        table = pq.read_table(f)
        print("Schema:")
        print(table.schema)
        print("\nColumns:", table.column_names)
except Exception as e:
    print(f"错误: {e}")
```

## 测试结果报告

测试完成后，请提供以下信息：

```
场景图片查看器测试结果
======================

测试日期: [填写日期]
测试环境: [环境描述]

基础功能:
- 单场景加载: [✓/✗]
- 批量加载: [✓/✗]
- HTML生成: [✓/✗]
- 浏览器显示: [✓/✗]

从Grid聚类查询:
- 查询成功: [✓/✗]
- 图片加载: [✓/✗]

性能:
- 单场景加载时间: [X]秒
- HTML文件大小: [X]MB

问题/建议:
[在此填写遇到的问题或改进建议]
```

## 详细文档

- 使用指南: `docs/scene_image_retrieval_guide.md`
- 测试说明: `docs/scene_image_retrieval_testing.md`
- 快速开始: `examples/dataset/bbox_examples/scene_image_viewer_README.md`

