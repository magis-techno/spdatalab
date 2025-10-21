# 场景图片检索工具 - 测试说明

## 测试环境要求

由于本地无法测试，需要在远端环境进行测试。测试环境需要：

1. **数据库访问**：能够访问 `transform.ods_t_data_fragment_datalake` 表
2. **OBS访问**：配置正确的OBS访问凭证
3. **Python依赖**：pandas, pyarrow, PIL/Pillow, sqlalchemy

## 快速测试步骤

### 步骤1：准备测试scene_id

首先需要一个有效的scene_id，可以从数据库查询：

```sql
-- 查询有scene_obs_path的场景
SELECT id AS scene_id, origin_name, scene_obs_path 
FROM transform.ods_t_data_fragment_datalake 
WHERE scene_obs_path IS NOT NULL 
LIMIT 10;
```

记录一个有效的 `scene_id`，例如：`scene_abc123456`

### 步骤2：测试单场景图片加载

```bash
# 使用Python测试基本功能
python -c "
from spdatalab.dataset.scene_image_retriever import SceneImageRetriever

# 替换为实际的scene_id
scene_id = 'scene_abc123456'

retriever = SceneImageRetriever()
frames = retriever.load_images_from_scene(scene_id, max_frames=2)

print(f'成功加载 {len(frames)} 帧图片')
if frames:
    frame = frames[0]
    print(f'场景: {frame.scene_id}')
    print(f'帧索引: {frame.frame_index}')
    print(f'时间戳: {frame.timestamp}')
    print(f'格式: {frame.image_format}')
    print(f'大小: {len(frame.image_data)/1024:.1f} KB')
"
```

**预期输出**：
```
查询 1 个场景的OBS路径...
✅ 成功查询到 1 个场景的OBS路径
场景OBS路径: obs://bucket/path/to/scene
找到 1 个parquet文件
成功解析 2 帧图片
✅ 场景 scene_abc123456 共加载 2 帧图片
成功加载 2 帧图片
场景: scene_abc123456
帧索引: 0
时间戳: 1697875200000
格式: png
大小: 234.5 KB
```

### 步骤3：测试HTML生成

```bash
# 使用命令行工具生成HTML报告
python examples/dataset/bbox_examples/view_cluster_images.py \
    --scene-ids scene_abc123456 \
    --frames-per-scene 3 \
    --output test_report.html
```

**预期输出**：
```
======================================================================
场景图片查看器
======================================================================

📋 待处理场景数: 1
🎬 每场景帧数: 3
📷 相机类型: CAM_FRONT_WIDE_ANGLE

======================================================================
开始加载图片...
======================================================================

...
✅ 批量加载完成: 成功 1 个，失败 0 个

======================================================================
生成HTML报告...
======================================================================

✅ HTML报告已生成: /path/to/test_report.html
   文件大小: 1.23 MB

======================================================================
✅ 处理完成！
======================================================================

📊 统计信息:
  成功加载场景数: 1
  总帧数: 3
  平均每场景帧数: 3.0

📄 HTML报告路径:
  /path/to/test_report.html

💡 提示: 在浏览器中打开HTML文件查看图片
======================================================================
```

### 步骤4：验证HTML文件

在浏览器中打开生成的 `test_report.html`，检查：

- [ ] 页面能够正常打开
- [ ] 显示场景标题和统计信息
- [ ] 缩略图正确显示
- [ ] 点击缩略图能查看全屏图片
- [ ] 元数据（帧索引、时间戳）显示正确
- [ ] 场景折叠/展开功能正常

## 完整测试场景

### 测试1：从Grid聚类结果查看图片

```bash
# 首先查询一个有效的grid_id
psql -h local_pg -U postgres -d postgres -c \
  "SELECT DISTINCT grid_id FROM public.grid_trajectory_segments LIMIT 10;"

# 使用查询到的grid_id测试
python examples/dataset/bbox_examples/view_cluster_images.py \
    --grid-id <grid_id> \
    --cluster-label 0 \
    --max-scenes 5 \
    --frames-per-scene 3 \
    --output grid_test.html
```

**检查点**：
- [ ] 能够从数据库查询到scene_ids
- [ ] 成功加载至少1个场景的图片
- [ ] HTML文件包含所有查询到的场景

### 测试2：批量处理多个场景

```bash
# 准备多个scene_id（用实际值替换）
python examples/dataset/bbox_examples/view_cluster_images.py \
    --scene-ids scene_001 scene_002 scene_003 scene_004 scene_005 \
    --frames-per-scene 2 \
    --output batch_test.html \
    --title "批量测试"
```

**检查点**：
- [ ] 成功处理多个场景
- [ ] 如果某些场景失败，不影响其他场景
- [ ] HTML报告中显示所有成功加载的场景

### 测试3：帧过滤功能

```python
# 创建测试脚本 test_frame_filtering.py
from spdatalab.dataset.scene_image_retriever import SceneImageRetriever
from spdatalab.dataset.scene_image_viewer import SceneImageHTMLViewer

scene_id = "scene_abc123456"  # 替换为实际scene_id
retriever = SceneImageRetriever()

# 测试1：加载前5帧
print("测试1：加载前5帧")
frames1 = retriever.load_images_from_scene(scene_id, max_frames=5)
print(f"  加载了 {len(frames1)} 帧\n")

# 测试2：加载指定索引的帧
print("测试2：加载指定索引的帧 [0, 5, 10]")
frames2 = retriever.load_images_from_scene(scene_id, frame_indices=[0, 5, 10])
print(f"  加载了 {len(frames2)} 帧")
for f in frames2:
    print(f"    帧 {f.frame_index}: {f.timestamp}")

# 生成对比报告
viewer = SceneImageHTMLViewer()
images_dict = {
    f"{scene_id}_first5": frames1,
    f"{scene_id}_selected": frames2
}
viewer.generate_html_report(
    images_dict,
    "frame_filtering_test.html",
    title="帧过滤测试"
)
print("\n✅ 测试完成，查看 frame_filtering_test.html")
```

运行测试：
```bash
python test_frame_filtering.py
```

### 测试4：错误处理

测试系统如何处理各种错误情况：

```python
# 创建测试脚本 test_error_handling.py
from spdatalab.dataset.scene_image_retriever import SceneImageRetriever

retriever = SceneImageRetriever()

# 测试1：无效的scene_id
print("测试1：无效的scene_id")
try:
    frames = retriever.load_images_from_scene("invalid_scene_id_12345")
    print(f"  结果: 加载了 {len(frames)} 帧（应该为0）")
except Exception as e:
    print(f"  异常: {e}")

# 测试2：空的scene_ids列表
print("\n测试2：空的scene_ids列表")
try:
    df = retriever.get_scene_obs_paths([])
except ValueError as e:
    print(f"  预期异常: {e}")

# 测试3：批量加载包含部分无效scene_id
print("\n测试3：批量加载（包含无效ID）")
mixed_ids = ["valid_scene_id", "invalid_scene_id_999"]
images = retriever.batch_load_images(mixed_ids, frames_per_scene=2)
print(f"  成功加载 {len(images)} 个场景")

print("\n✅ 错误处理测试完成")
```

运行测试：
```bash
python test_error_handling.py
```

## 单元测试

运行pytest单元测试：

```bash
# 安装测试依赖
pip install pytest

# 运行所有单元测试
python -m pytest tests/test_scene_image_retriever.py -v

# 运行特定测试类
python -m pytest tests/test_scene_image_retriever.py::TestImageFrame -v

# 运行特定测试
python -m pytest tests/test_scene_image_retriever.py::TestSceneImageRetriever::test_detect_image_format_png -v
```

## 性能测试

测试大规模场景处理的性能：

```bash
# 创建性能测试脚本 test_performance.py
import time
from spdatalab.dataset.scene_image_retriever import SceneImageRetriever
from spdatalab.dataset.scene_image_viewer import SceneImageHTMLViewer

# 准备测试数据（使用实际scene_ids）
scene_ids = [...]  # 10-20个有效的scene_ids

print(f"性能测试：{len(scene_ids)} 个场景")

# 测试加载时间
retriever = SceneImageRetriever()
start_time = time.time()

images = retriever.batch_load_images(scene_ids, frames_per_scene=3)

load_time = time.time() - start_time
print(f"加载时间: {load_time:.2f} 秒")
print(f"平均每场景: {load_time/len(scene_ids):.2f} 秒")

# 测试HTML生成时间
viewer = SceneImageHTMLViewer()
start_time = time.time()

report_path = viewer.generate_html_report(
    images,
    "performance_test.html",
    title="性能测试"
)

html_time = time.time() - start_time
print(f"HTML生成时间: {html_time:.2f} 秒")

# 报告文件大小
import os
file_size_mb = os.path.getsize(report_path) / (1024 * 1024)
print(f"HTML文件大小: {file_size_mb:.2f} MB")

print("\n✅ 性能测试完成")
```

运行测试：
```bash
python test_performance.py
```

**性能基准**（参考值）：
- 单场景加载：2-5秒（取决于网络和parquet大小）
- HTML生成：< 1秒（对于10个场景，每场景3帧）
- 文件大小：约1-2MB每场景（3帧）

## 测试检查清单

完成以下测试后，工具即可投入使用：

### 基础功能
- [ ] 数据库查询scene_obs_path成功
- [ ] OBS文件访问正常
- [ ] Parquet文件读取成功
- [ ] 图片二进制数据解码正常
- [ ] 支持PNG和JPEG格式

### 核心功能
- [ ] 单场景图片加载成功
- [ ] 批量场景图片加载成功
- [ ] 帧数限制功能正常（max_frames）
- [ ] 帧索引过滤功能正常（frame_indices）
- [ ] HTML报告生成成功

### HTML查看器
- [ ] HTML文件在浏览器中正常打开
- [ ] 缩略图显示正常
- [ ] 全屏图片查看功能正常
- [ ] 场景折叠/展开功能正常
- [ ] 元数据显示正确
- [ ] 统计信息正确

### 命令行工具
- [ ] --scene-ids 参数正常工作
- [ ] --grid-id 参数正常工作
- [ ] --analysis-id 参数正常工作
- [ ] 输出路径自定义正常
- [ ] 标题自定义正常

### 错误处理
- [ ] 无效scene_id不导致程序崩溃
- [ ] OBS访问失败有清晰错误提示
- [ ] 部分场景失败不影响其他场景
- [ ] 空结果有适当提示

### 性能
- [ ] 10个场景在合理时间内完成（< 2分钟）
- [ ] HTML文件大小在可接受范围（< 100MB）
- [ ] 内存使用正常（不超过可用内存）

## 常见问题排查

### 问题1：ImportError: No module named 'moxing'

**解决方案**：
```bash
pip install moxing
```

### 问题2：数据库连接失败

**排查步骤**：
```bash
# 测试数据库连接
python -c "
from spdatalab.common.io_hive import hive_cursor
with hive_cursor() as cur:
    cur.execute('SELECT 1')
    print('数据库连接正常')
"
```

### 问题3：OBS访问权限错误

**排查步骤**：
```bash
# 检查环境变量
python -c "
import os
print('S3_ENDPOINT:', os.getenv('S3_ENDPOINT'))
print('ACCESS_KEY_ID:', os.getenv('ACCESS_KEY_ID'))
print('SECRET_ACCESS_KEY:', os.getenv('SECRET_ACCESS_KEY')[:10] + '...')
"
```

### 问题4：parquet文件结构不匹配

**排查步骤**：
```python
# 检查实际的parquet结构
import pyarrow.parquet as pq
from spdatalab.common.file_utils import open_file

# 使用实际的parquet路径
obs_path = "obs://bucket/path/to/scene/samples/CAM_FRONT_WIDE_ANGLE/data.parquet"

with open_file(obs_path, 'rb') as f:
    table = pq.read_table(f)
    print("Schema:")
    print(table.schema)
    print("\nFirst row:")
    print(table.to_pandas().head(1))
```

## 测试报告模板

测试完成后，请填写以下报告：

```
场景图片检索工具测试报告
========================

测试日期：YYYY-MM-DD
测试环境：[远端环境描述]
测试人员：[姓名]

基础功能测试：
- 数据库查询：[✓/✗] [备注]
- OBS访问：[✓/✗] [备注]
- Parquet读取：[✓/✗] [备注]
- 图片解码：[✓/✗] [备注]

核心功能测试：
- 单场景加载：[✓/✗] [备注]
- 批量加载：[✓/✗] [备注]
- 帧过滤：[✓/✗] [备注]
- HTML生成：[✓/✗] [备注]

性能测试：
- 10场景加载时间：[XX秒]
- HTML文件大小：[XX MB]
- 内存使用：[正常/异常]

发现的问题：
1. [问题描述]
2. [问题描述]

建议：
1. [改进建议]
2. [改进建议]

总体评价：[通过/需改进]
```

## 联系支持

如测试过程中遇到问题，请提供：
1. 错误信息和堆栈跟踪
2. 使用的scene_id（可脱敏）
3. 环境信息（Python版本、依赖版本）
4. 测试命令和参数

