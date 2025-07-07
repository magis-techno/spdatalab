# 轨迹数据生成指南（简化版）

## 功能概述

轨迹生成功能支持基于具体的场景列表生成详细的轨迹数据。这种简单直接的方式特别适合在QGIS中分析bbox后，基于分析结果生成感兴趣场景的轨迹。

## 核心工作流程

**推荐的分析流程：**
1. 生成bbox概览数据
2. 在QGIS中分析bbox分布  
3. 导出感兴趣的场景列表
4. 基于场景列表生成轨迹

## 处理模式

### 1. 初始处理模式

#### 模式A：仅边界框（推荐，快速概览）
```bash
# 生成bbox概览，用于后续QGIS分析
python -m spdatalab.dataset.bbox --input data/scenes.txt --mode bbox
```

#### 模式B：仅轨迹（基于输入文件）
```bash
# 基于输入文件中的所有场景生成轨迹
python -m spdatalab.dataset.bbox --input data/scenes.txt --mode trajectory
```

#### 模式C：同时生成（适合小数据集）
```bash
# 同时生成bbox和轨迹
python -m spdatalab.dataset.bbox --input data/scenes.txt --mode both
```

### 2. 基于场景列表的轨迹生成（核心功能）

这是主要的使用方式：在QGIS中分析bbox后，导出感兴趣的场景列表，然后生成轨迹。

#### 从文件读取场景列表
```bash
# 从文件读取场景列表（推荐方式）
python -m spdatalab.dataset.bbox --generate-trajectories \
    --scenes selected_scenes.txt \
    --trajectory-table clips_trajectory_selected
```

**scene_list.txt 文件格式：**
```
# 感兴趣的场景列表（支持注释行）
scene_token_001
scene_token_002  
scene_token_003
# 更多场景...
scene_token_100
```

#### 直接指定场景列表
```bash
# 逗号分隔的场景列表（适合少量场景）
python -m spdatalab.dataset.bbox --generate-trajectories \
    --scenes "scene_token_1,scene_token_2,scene_token_3" \
    --trajectory-table clips_trajectory_test
```

#### 单个场景
```bash
# 单个场景的轨迹
python -m spdatalab.dataset.bbox --generate-trajectories \
    --scenes scene_token_001 \
    --trajectory-table clips_trajectory_single
```

## 从QGIS导出场景列表

### 方法1：属性表导出
1. 在QGIS中打开bbox图层
2. 使用空间查询或属性过滤选择感兴趣的记录
3. 在属性表中选择 `scene_token` 列
4. 复制选中的值到文本文件

### 方法2：SQL查询导出
```sql
-- 在QGIS中执行SQL查询，导出结果
SELECT scene_token 
FROM clips_bbox_unified 
WHERE city_id = 'beijing' AND all_good = true;
```

### 方法3：Python脚本导出
```python
# 在QGIS Python控制台中运行
layer = iface.activeLayer()
selected_features = layer.selectedFeatures()
scene_tokens = [f['scene_token'] for f in selected_features]

# 保存到文件
with open('/path/to/selected_scenes.txt', 'w') as f:
    for token in scene_tokens:
        f.write(f"{token}\n")
```

## 轨迹表结构

生成的轨迹表包含以下字段：

| 字段名 | 类型 | 描述 |
|--------|------|------|
| id | serial | 主键 |
| scene_token | text | 场景token |
| data_name | text | 数据名称 |
| trajectory | geometry | 轨迹线（LineString） |
| trajectory_length | integer | 轨迹长度（米） |
| start_time | double precision | 开始时间 |
| end_time | double precision | 结束时间 |
| max_speed | double precision | 最大速度（m/s） |
| all_good | boolean | 数据质量标记 |
| created_at | timestamp | 创建时间 |

## 实际使用场景

### 场景1：事故热点分析
```bash
# 1. 生成所有事故场景的bbox
python -m spdatalab.dataset.bbox --input accident_scenes.txt --mode bbox

# 2. 在QGIS中识别事故热点区域，导出场景列表到 hotspot_scenes.txt

# 3. 为热点场景生成详细轨迹
python -m spdatalab.dataset.bbox --generate-trajectories \
    --scenes hotspot_scenes.txt \
    --trajectory-table accident_trajectories_hotspot
```

### 场景2：算法验证工作流
```bash
# 1. 生成测试数据集的bbox概览
python -m spdatalab.dataset.bbox --input test_dataset.txt --mode bbox

# 2. 在QGIS中选择高质量场景，导出到 validation_scenes.txt

# 3. 生成验证轨迹
python -m spdatalab.dataset.bbox --generate-trajectories \
    --scenes validation_scenes.txt \
    --trajectory-table trajectories_validation

# 4. 选择边界情况场景，导出到 edge_case_scenes.txt

# 5. 生成边界情况轨迹
python -m spdatalab.dataset.bbox --generate-trajectories \
    --scenes edge_case_scenes.txt \
    --trajectory-table trajectories_edge_cases
```

### 场景3：分城市分析
```bash
# 1. 生成全国数据的bbox概览
python -m spdatalab.dataset.bbox --input national_data.txt --mode bbox

# 2. 在QGIS中按城市筛选，分别导出：
#    - beijing_scenes.txt
#    - shanghai_scenes.txt
#    - guangzhou_scenes.txt

# 3. 分别为各城市生成轨迹
python -m spdatalab.dataset.bbox --generate-trajectories \
    --scenes beijing_scenes.txt \
    --trajectory-table trajectories_beijing

python -m spdatalab.dataset.bbox --generate-trajectories \
    --scenes shanghai_scenes.txt \
    --trajectory-table trajectories_shanghai
```

## 问题单轨迹生成

对于问题单数据，工作流程类似：

```bash
# 1. 生成问题单bbox
python -m spdatalab.dataset.bbox --input issue_tickets.txt --issue-tickets

# 2. 在QGIS中分析问题单分布，导出场景列表

# 3. 基于问题单场景生成轨迹（注意：问题单的scene_token可能不在普通轨迹数据中）
python -m spdatalab.dataset.bbox --generate-trajectories \
    --scenes issue_scenes.txt \
    --trajectory-table clips_trajectory_issues
```

## 性能和存储优势

### 存储对比
- **bbox数据**: ~100 bytes/记录  
- **轨迹数据**: ~10KB/记录
- **按需生成优势**: 当只分析1%的数据时，可节省99%的存储空间

### 分析效率
- **第一步**: bbox快速概览，几分钟完成
- **第二步**: QGIS可视化分析，快速定位感兴趣区域  
- **第三步**: 针对性轨迹生成，只处理需要的数据
- **第四步**: 详细轨迹分析

## 最佳实践

1. **优先生成bbox** - 使用 `--mode bbox` 快速概览
2. **充分利用QGIS** - 可视化分析bbox分布，识别模式
3. **合理组织场景列表** - 使用有意义的文件名，如 `beijing_high_quality_scenes.txt`
4. **适当的表命名** - 使用描述性的轨迹表名，如 `clips_trajectory_beijing_accidents`
5. **支持注释** - 在场景列表文件中使用 `#` 添加注释说明

## 常见问题

### Q: 如何批量处理多个场景列表？
```bash
#!/bin/bash
# 批量处理脚本
for scene_file in scenes_*.txt; do
    table_name="clips_trajectory_$(basename $scene_file .txt)"
    python -m spdatalab.dataset.bbox --generate-trajectories \
        --scenes "$scene_file" \
        --trajectory-table "$table_name"
done
```

### Q: 如何验证场景列表中的场景是否有轨迹数据？
在生成轨迹时，系统会自动跳过没有轨迹数据的场景，并在日志中显示实际生成的轨迹数量。

### Q: 生成的轨迹表过大怎么办？
- 分批处理：将大的场景列表拆分成多个小文件
- 按区域分表：为不同区域创建独立的轨迹表
- 及时清理：删除不再需要的临时轨迹表

## 命令速查

```bash
# 生成bbox概览
python -m spdatalab.dataset.bbox --input data.txt --mode bbox

# 从文件生成轨迹
python -m spdatalab.dataset.bbox --generate-trajectories \
    --scenes scene_list.txt \
    --trajectory-table my_trajectories

# 从逗号列表生成轨迹
python -m spdatalab.dataset.bbox --generate-trajectories \
    --scenes "token1,token2,token3" \
    --trajectory-table test_trajectories

# 问题单处理
python -m spdatalab.dataset.bbox --input issues.txt --issue-tickets

# 同时生成bbox和轨迹（小数据集）
python -m spdatalab.dataset.bbox --input data.txt --mode both
```

这种简化的设计更贴近实际使用场景，避免了复杂的查询逻辑，让用户能够直接控制要生成轨迹的具体场景。 